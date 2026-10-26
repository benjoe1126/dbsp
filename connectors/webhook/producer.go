package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/datamodel"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// maxBodySize caps the request body read from a client.
const maxBodySize = 4 << 20

// shutdownGrace bounds how long the in-flight requests may take to finish
// after the producer is stopped.
const shutdownGrace = 5 * time.Second

// Producer is an HTTP server that publishes each request body as a
// document to its topic and answers the request with the response the
// circuit routes back to ResponseTopic(server, uid).
type Producer struct {
	*dbspruntime.BaseProducer

	topic    string
	server   string
	address  string
	path     string
	certFile string
	keyFile  string
	uidPath  string
	timeout  time.Duration

	runtime *dbspruntime.Runtime
	log     logr.Logger

	// inflight holds the uids of the requests awaiting a response, so a
	// concurrent duplicate cannot steal or retract another's response.
	mu       sync.Mutex
	inflight map[string]struct{}

	// addr is the bound listen address, set once Start has listened.
	addr  net.Addr
	ready chan struct{}
}

var _ dbspruntime.Producer = (*Producer)(nil)

// NewProducer builds the webhook server source publishing to topic.
func NewProducer(topic string, spec ProducerSpec, deps Deps) (*Producer, error) {
	if deps.Runtime == nil {
		return nil, fmt.Errorf("webhook: runtime is required")
	}
	if topic == "" {
		return nil, fmt.Errorf("webhook: topic is required")
	}
	if spec.Address == "" {
		return nil, fmt.Errorf("webhook: address is required")
	}
	if (spec.CertFile == "") != (spec.KeyFile == "") {
		return nil, fmt.Errorf("webhook: certFile and keyFile must be given together")
	}
	if spec.Server == "" {
		spec.Server = DefaultServer
	}
	if spec.Path == "" {
		spec.Path = DefaultPath
	}
	if spec.UIDPath == "" {
		spec.UIDPath = DefaultRequestUIDPath
	}
	timeout := DefaultTimeout
	if spec.Timeout != "" {
		d, err := time.ParseDuration(spec.Timeout)
		if err != nil {
			return nil, fmt.Errorf("webhook: timeout: %w", err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("webhook: timeout must be positive")
		}
		timeout = d
	}

	logger := deps.logger().WithName("webhook-producer").WithValues("server", spec.Server, "topic", topic)
	base, err := dbspruntime.NewBaseProducer(dbspruntime.BaseProducerConfig{
		Name:          fmt.Sprintf("webhook/server/%s/%s", spec.Server, topic),
		Publisher:     deps.Runtime.NewPublisher(),
		ErrorReporter: deps.Runtime,
		Logger:        logger,
		Topics:        []string{topic},
	})
	if err != nil {
		return nil, err
	}

	return &Producer{
		BaseProducer: base,
		topic:        topic,
		server:       spec.Server,
		address:      spec.Address,
		path:         spec.Path,
		certFile:     spec.CertFile,
		keyFile:      spec.KeyFile,
		uidPath:      spec.UIDPath,
		timeout:      timeout,
		runtime:      deps.Runtime,
		log:          logger,
		inflight:     map[string]struct{}{},
		ready:        make(chan struct{}),
	}, nil
}

// Name returns the producer's unique component name.
func (p *Producer) Name() string { return p.BaseProducer.Name() }

// String implements fmt.Stringer.
func (p *Producer) String() string {
	if p == nil {
		return "producer<webhook>{<nil>}"
	}
	return fmt.Sprintf("producer<webhook>{name=%q, server=%q, address=%q, topic=%q}", p.Name(), p.server, p.address, p.topic)
}

// MarshalJSON provides a stable machine-readable representation.
func (p *Producer) MarshalJSON() ([]byte, error) {
	if p == nil {
		return json.Marshal(map[string]any{"component": "producer", "type": "webhook", "nil": true})
	}
	return json.Marshal(map[string]any{
		"component": "producer",
		"type":      "webhook",
		"name":      p.Name(),
		"server":    p.server,
		"address":   p.address,
		"path":      p.path,
		"topic":     p.topic,
	})
}

// Addr blocks until the server listens and returns its bound address, or
// returns nil when ctx ends first.
func (p *Producer) Addr(ctx context.Context) net.Addr {
	select {
	case <-p.ready:
		return p.addr
	case <-ctx.Done():
		return nil
	}
}

// Start serves HTTP until ctx is cancelled. Failing to listen is a
// critical error.
func (p *Producer) Start(ctx context.Context) error {
	defer p.ClosePublisher()

	ln, err := net.Listen("tcp", p.address)
	if err != nil {
		return fmt.Errorf("webhook: listen %s: %w", p.address, err)
	}
	p.addr = ln.Addr()
	close(p.ready)

	mux := http.NewServeMux()
	mux.HandleFunc(p.path, p.handle)
	srv := &http.Server{
		Handler:           mux,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ReadHeaderTimeout: 30 * time.Second,
		ErrorLog:          log.New(io.Discard, "", 0),
	}

	errCh := make(chan error, 1)
	go func() {
		if p.certFile != "" {
			errCh <- srv.ServeTLS(ln, p.certFile, p.keyFile)
		} else {
			errCh <- srv.Serve(ln)
		}
	}()
	p.log.Info("webhook server listening", "addr", p.addr.String(), "path", p.path, "tls", p.certFile != "")

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("webhook: serve: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
		}
		return nil
	}
}

// handle runs one request/response round trip through the circuit.
func (p *Producer) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize+1))
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if len(body) > maxBodySize {
		http.Error(w, "Request Entity Too Large", http.StatusRequestEntityTooLarge)
		return
	}

	doc := &dbspunstructured.Unstructured{}
	if err := doc.UnmarshalJSON(body); err != nil || doc.Fields() == nil {
		http.Error(w, "request body must be a JSON object", http.StatusBadRequest)
		return
	}
	uid, err := documentUID(doc, p.uidPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("request id: %v", err), http.StatusBadRequest)
		return
	}

	if !p.acquire(uid) {
		http.Error(w, fmt.Sprintf("request %q is already in flight", uid), http.StatusConflict)
		return
	}
	defer p.release(uid)

	// Subscribe before publishing: the pub/sub retains nothing, so a
	// response routed before the subscription would be lost.
	sub := p.runtime.NewSubscriber()
	sub.Subscribe(ResponseTopic(p.server, uid))
	ctx, cancel := context.WithTimeout(r.Context(), p.timeout)
	defer cancel()
	stop := context.AfterFunc(ctx, sub.UnsubscribeAll)
	defer func() {
		stop()
		sub.UnsubscribeAll()
	}()

	if err := p.publish(doc, 1); err != nil {
		p.HandleError(fmt.Errorf("publish request %s: %w", uid, err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	// Retract the request once answered (or abandoned), so the circuit
	// holds no state for it.
	defer func() {
		if err := p.publish(doc, -1); err != nil {
			p.HandleError(fmt.Errorf("retract request %s: %w", uid, err))
		}
	}()

	resp, ok := p.await(sub)
	if !ok {
		if r.Context().Err() == nil {
			p.HandleError(fmt.Errorf("request %s: no response within %s", uid, p.timeout))
			http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		}
		return
	}

	out, err := json.Marshal(resp)
	if err != nil {
		p.HandleError(fmt.Errorf("marshal response %s: %w", uid, err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

// await returns the first positive-weight document routed to the
// subscription, or false when the subscription is closed first.
func (p *Producer) await(sub dbspruntime.Subscriber) (any, bool) {
	for {
		evt, ok := sub.Next()
		if !ok {
			return nil, false
		}
		var resp any
		evt.Data.Iter(func(doc datamodel.Document, weight zset.Weight) bool {
			if weight > 0 {
				resp = doc
				return false
			}
			return true
		})
		if resp != nil {
			return resp, true
		}
	}
}

func (p *Producer) publish(doc *dbspunstructured.Unstructured, weight zset.Weight) error {
	zs := zset.New()
	zs.Insert(doc, weight)
	dbspruntime.LogFlowEvent(p.log, "producer.emit", "producer", p.String(), "output", p.topic, "", zs, nil)
	return p.Publish(dbspruntime.Event{Name: p.topic, Data: zs})
}

func (p *Producer) acquire(uid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.inflight[uid]; ok {
		return false
	}
	p.inflight[uid] = struct{}{}
	return true
}

func (p *Producer) release(uid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.inflight, uid)
}
