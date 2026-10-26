package webhook

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/datamodel"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// Consumer reads the circuit's response documents and routes each to
// ResponseTopic(server, uid), where the HTTP request with that uid waits
// for it. Retractions are dropped: a response is delivered once, and the
// retraction that follows the request's own retraction has no reader.
type Consumer struct {
	*dbspruntime.BaseConsumer
	dbspruntime.Publisher

	topic   string
	server  string
	uidPath string

	log logr.Logger
}

var _ dbspruntime.Consumer = (*Consumer)(nil)

// NewConsumer builds the webhook response target consuming from topic.
func NewConsumer(topic string, spec ConsumerSpec, deps Deps) (*Consumer, error) {
	if deps.Runtime == nil {
		return nil, fmt.Errorf("webhook: runtime is required")
	}
	if topic == "" {
		return nil, fmt.Errorf("webhook: topic is required")
	}
	if spec.Server == "" {
		spec.Server = DefaultServer
	}
	if spec.UIDPath == "" {
		spec.UIDPath = DefaultResponseUIDPath
	}

	logger := deps.logger().WithName("webhook-consumer").WithValues("server", spec.Server, "topic", topic)
	base, err := dbspruntime.NewBaseConsumer(dbspruntime.BaseConsumerConfig{
		Name:          fmt.Sprintf("webhook/responder/%s/%s", spec.Server, topic),
		Subscriber:    deps.Runtime.NewSubscriber(),
		ErrorReporter: deps.Runtime,
		Logger:        logger,
		Topics:        []string{topic},
	})
	if err != nil {
		return nil, err
	}

	return &Consumer{
		BaseConsumer: base,
		Publisher:    deps.Runtime.NewPublisher(),
		topic:        topic,
		server:       spec.Server,
		uidPath:      spec.UIDPath,
		log:          logger,
	}, nil
}

// Name returns the consumer's unique component name.
func (c *Consumer) Name() string { return c.BaseConsumer.Name() }

// String implements fmt.Stringer.
func (c *Consumer) String() string {
	if c == nil {
		return "consumer<webhook>{<nil>}"
	}
	return fmt.Sprintf("consumer<webhook>{name=%q, server=%q, topic=%q}", c.Name(), c.server, c.topic)
}

// MarshalJSON provides a stable machine-readable representation.
func (c *Consumer) MarshalJSON() ([]byte, error) {
	if c == nil {
		return json.Marshal(map[string]any{"component": "consumer", "type": "webhook", "nil": true})
	}
	return json.Marshal(map[string]any{
		"component": "consumer",
		"type":      "webhook",
		"name":      c.Name(),
		"server":    c.server,
		"topic":     c.topic,
	})
}

// Start runs the consumer event loop until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	defer func() {
		if closer, ok := c.Publisher.(dbspruntime.PublisherCloser); ok {
			closer.Close()
		}
	}()
	return c.Run(ctx, c)
}

// Consume routes the positive-weight documents of one circuit output
// delta to their per-request response topics.
func (c *Consumer) Consume(_ context.Context, event dbspruntime.Event) error {
	dbspruntime.LogFlowEvent(c.log, "consumer.receive", "consumer", c.String(), "input", event.Name, "", event.Data, nil)

	var errs []error
	event.Data.Iter(func(doc datamodel.Document, weight zset.Weight) bool {
		if weight <= 0 {
			return true
		}
		uid, err := documentUID(doc, c.uidPath)
		if err != nil {
			errs = append(errs, fmt.Errorf("response id: %w", err))
			return true
		}
		zs := zset.New()
		zs.Insert(doc, 1)
		if err := c.Publish(dbspruntime.Event{Name: ResponseTopic(c.server, uid), Data: zs}); err != nil {
			errs = append(errs, fmt.Errorf("route response %s: %w", uid, err))
		}
		return true
	})
	if len(errs) > 0 {
		return fmt.Errorf("webhook: %v", errs)
	}
	return nil
}
