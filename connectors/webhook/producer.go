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
	"slices"
	"strings"
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
// document to the topic routed to its path and answers the request with the response the
// circuit routes back to ResponseTopic(server, uid).
type Producer struct {
	*dbspruntime.BaseProducer

	routes   map[string]string // HTTP path -> topic
	server   string
	address  string
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

// NewProducer builds the webhook server source publishing the requests
// on spec.Path to topic and those on each of spec.Routes to its topic.
func NewProducer(topic string, spec ProducerSpec, deps Deps) (*Producer, error) {
	if deps.Runtime == nil {
		return nil, fmt.Errorf("webhook: runtime is required")
	}
	if topic == "" && len(spec.Routes) == 0 {
		return nil, fmt.Errorf("webhook: topic or routes is required")
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
	routes := map[string]string{}
	for path, t := range spec.Routes {
		if path == "" || t == "" {
			return nil, fmt.Errorf("webhook: route %q -> %q: path and topic must be non-empty", path, t)
		}
		routes[path] = t
	}
	if topic != "" {
		if _, ok := routes[spec.Path]; ok {
			return nil, fmt.Errorf("webhook: path %q is routed twice", spec.Path)
		}
		routes[spec.Path] = topic
	}
	topics := slices.Sorted(func(yield func(string) bool) {
		for _, t := range routes {
			if !yield(t) {
				return
			}
		}
	})
	topics = slices.Compact(topics)
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

	logger := deps.logger().WithName("webhook-producer").WithValues("server", spec.Server, "topics", topics)
	base, err := dbspruntime.NewBaseProducer(dbspruntime.BaseProducerConfig{
		Name:          fmt.Sprintf("webhook/server/%s/%s", spec.Server, strings.Join(topics, ",")),
		Publisher:     deps.Runtime.NewPublisher(),
		ErrorReporter: deps.Runtime,
		Logger:        logger,
		Topics:        topics,
	})
	if err != nil {
		return nil, err
	}

	return &Producer{
		BaseProducer: base,
		routes:       routes,
		server:       spec.Server,
		address:      spec.Address,
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
	return fmt.Sprintf("producer<webhook>{name=%q, server=%q, address=%q, routes=%v}", p.Name(), p.server, p.address, p.routes)
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
		"routes":    p.routes,
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
	for path, topic := range p.routes {
		mux.HandleFunc(path, p.handler(topic))
	}
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
	p.log.Info("webhook server listening", "addr", p.addr.String(), "routes", p.routes, "tls", p.certFile != "")

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

// handler returns the HTTP handler publishing its requests to topic.
func (p *Producer) handler(topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { p.handle(topic, w, r) }
}

// handle runs one request/response round trip through the circuit,
// publishing the request to topic.
func (p *Producer) handle(topic string, w http.ResponseWriter, r *http.Request) {
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

	if err := p.publish(topic, doc, 1); err != nil {
		p.HandleError(fmt.Errorf("publish request %s: %w", uid, err))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	// Retract the request once answered (or abandoned), so the circuit
	// holds no state for it.
	defer func() {
		if err := p.publish(topic, doc, -1); err != nil {
			p.HandleError(fmt.Errorf("retract request %s: %w", uid, err))
		}
	}()

	resp, ok := p.await(sub)
	p.log.Info("message received by producer", "msg", resp)
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

func (p *Producer) publish(topic string, doc *dbspunstructured.Unstructured, weight zset.Weight) error {
	zs := zset.New()
	zs.Insert(doc, weight)
	dbspruntime.LogFlowEvent(p.log, "producer.emit", "producer", p.String(), "output", topic, "", zs, nil)
	return p.Publish(dbspruntime.Event{Name: topic, Data: zs})
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
