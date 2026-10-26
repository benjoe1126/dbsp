package webhook

import (
	"encoding/json"
	"fmt"

	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/spec"
)

// Group is the API group routing bindings to the webhook connector.
const Group = "webhook.connector.dcontroller.io"

// NewFactory returns the webhook connector's binding factory: HTTP server
// sources publishing requests, and targets routing the circuit's
// responses back to the waiting requests. A source and the target
// answering it name the same parameters.server.
func NewFactory() runtime.ConnectorFactory {
	return runtime.ConnectorFactory{
		Name:   "webhook",
		Groups: []string{Group},
		NewSource: func(rt *runtime.Runtime, s *spec.Source, topic string) (runtime.Runnable, error) {
			if s.Type != "" && s.Type != spec.Watcher {
				return nil, fmt.Errorf("unknown webhook source type %q", s.Type)
			}
			var ps ProducerSpec
			if s.Parameters != nil {
				if err := json.Unmarshal(*s.Parameters, &ps); err != nil {
					return nil, fmt.Errorf("parameters: %w", err)
				}
			}
			return NewProducer(topic, ps, Deps{Runtime: rt, Logger: rt.Logger()})
		},
		NewTarget: func(rt *runtime.Runtime, t *spec.Target, topic string) (runtime.Runnable, error) {
			if t.Type != "" && t.Type != spec.Updater {
				return nil, fmt.Errorf("unknown webhook target type %q", t.Type)
			}
			var cs ConsumerSpec
			if t.Parameters != nil {
				if err := json.Unmarshal(*t.Parameters, &cs); err != nil {
					return nil, fmt.Errorf("parameters: %w", err)
				}
			}
			return NewConsumer(topic, cs, Deps{Runtime: rt, Logger: rt.Logger()})
		},
	}
}
