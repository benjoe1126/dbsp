package js

import (
	"fmt"

	"github.com/dop251/goja"

	"github.com/l7mp/dbsp/connectors/webhook"
)

// The webhook namespace binds the webhook connector:
// `webhook.serve(topic, {address, server, path, ...})` starts an HTTP server
// publishing each request to topic (or `webhook.serve({address, routes:
// {path: topic, ...}})` to feed one topic per HTTP path),`webhook.respond(topic, {server, uidPath})`
// routes the response documents on topic back to the waiting requests.

func (v *VM) newWebhookNamespace() (*goja.Object, error) {
	obj := v.rt.NewObject()
	if err := obj.Set("serve", v.wrap(v.serveWebhook)); err != nil {
		return nil, err
	}
	if err := obj.Set("respond", v.wrap(v.respondWebhook)); err != nil {
		return nil, err
	}
	if err := obj.Set("toJSON", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(map[string]any{
			"kind": "webhook",
			"apis": []string{"serve", "respond"},
		}), nil
	})); err != nil {
		return nil, err
	}
	return obj, nil
}

// serveWebhook accepts `serve(topic, opts)` or, with opts.routes, `serve(opts)`.
func (v *VM) serveWebhook(call goja.FunctionCall) (goja.Value, error) {
	topic, opts := "", call.Argument(0)
	if t, ok := opts.Export().(string); ok {
		topic, opts = t, call.Argument(1)
	}
	var spec webhook.ProducerSpec
	if err := decodeOptionValue(opts, &spec); err != nil {
		return nil, fmt.Errorf("webhook.serve options: %w", err)
	}
	p, err := webhook.NewProducer(topic, spec, webhook.Deps{Runtime: v.runtime, Logger: v.logger})
	if err != nil {
		return nil, err
	}
	if err := v.runtime.Add(p); err != nil {
		return nil, fmt.Errorf("webhook.serve: register producer: %w", err)
	}
	return v.boundHandle(p, "webhook.serve", topic, spec), nil
}

func (v *VM) respondWebhook(call goja.FunctionCall) (goja.Value, error) {
	topic, ok := call.Argument(0).Export().(string)
	if !ok || topic == "" {
		return nil, fmt.Errorf("webhook.respond(topic, opts): expected non-empty string topic, got %s", describeCall(call))
	}
	var spec webhook.ConsumerSpec
	if err := decodeOptionValue(call.Argument(1), &spec); err != nil {
		return nil, fmt.Errorf("webhook.respond options: %w", err)
	}
	c, err := webhook.NewConsumer(topic, spec, webhook.Deps{Runtime: v.runtime, Logger: v.logger})
	if err != nil {
		return nil, err
	}
	if err := v.runtime.Add(c); err != nil {
		return nil, fmt.Errorf("webhook.respond: register consumer: %w", err)
	}
	return v.boundHandle(c, "webhook.respond", topic, spec), nil
}
