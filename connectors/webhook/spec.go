// Package webhook provides the webhook connector: an HTTP server source
// that turns each incoming request into a document on a topic, and a
// target that routes the circuit's response documents back to the HTTP
// request waiting for them.
//
// Request and response are correlated through the request's unique ID
// (the uid of a Kubernetes AdmissionReview by default), without breaking
// the topic model:
//
//  1. the server subscribes to ResponseTopic(server, uid),
//  2. publishes the request body as a +1 document to the source topic,
//  3. the circuit computes a response document carrying the same uid,
//  4. the target reads the circuit output and republishes each response
//     to ResponseTopic(server, uid),
//  5. the server writes the first response to the HTTP client and
//     retracts the request (-1), so the circuit holds no stale state.
//
// Source and target meet only through the pub/sub, by sharing the
// server name in their parameters.
package webhook

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/datamodel"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

const (
	// DefaultServer is the server name used when the parameters name none.
	DefaultServer = "default"
	// DefaultRequestUIDPath locates the request ID in a request document
	// (the AdmissionReview request uid).
	DefaultRequestUIDPath = "$.request.uid"
	// DefaultResponseUIDPath locates the request ID in a response
	// document (the AdmissionReview response uid).
	DefaultResponseUIDPath = "$.response.uid"
	// DefaultPath is the HTTP path the server answers on.
	DefaultPath = "/"
	// DefaultTimeout bounds how long a request waits for its response.
	DefaultTimeout = 10 * time.Second
)

// ResponseTopic names the per-request topic the response for uid is
// routed to on the given server.
func ResponseTopic(server, uid string) string {
	return fmt.Sprintf("webhook/%s/response/%s", server, uid)
}

// ProducerSpec is the serialized configuration of the webhook server
// source.
type ProducerSpec struct {
	// Server names the server; the target answering it must name the
	// same. Default: DefaultServer.
	Server string `json:"server,omitempty"`
	// Address is the listen address, e.g. ":8443". Required.
	Address string `json:"address"`
	// Path is the HTTP path served. Default: DefaultPath.
	Path string `json:"path,omitempty"`
	// CertFile and KeyFile, when both given, switch the server to HTTPS
	// (Kubernetes admission webhooks require it).
	CertFile string `json:"certFile,omitempty"`
	KeyFile  string `json:"keyFile,omitempty"`
	// UIDPath is the JSONPath of the request ID in the request body.
	// Default: DefaultRequestUIDPath.
	UIDPath string `json:"uidPath,omitempty"`
	// Timeout is how long a request waits for its response, as a Go
	// duration string. Default: DefaultTimeout.
	Timeout string `json:"timeout,omitempty"`
}

// ConsumerSpec is the serialized configuration of the webhook response
// target.
type ConsumerSpec struct {
	// Server names the server whose requests this target answers.
	// Default: DefaultServer.
	Server string `json:"server,omitempty"`
	// UIDPath is the JSONPath of the request ID in a response document.
	// Default: DefaultResponseUIDPath.
	UIDPath string `json:"uidPath,omitempty"`
}

// Deps carries the non-serializable runtime dependencies of the webhook
// components.
type Deps struct {
	Runtime *dbspruntime.Runtime
	Logger  logr.Logger
}

func (d Deps) logger() logr.Logger {
	if d.Logger.GetSink() == nil {
		return logr.Discard()
	}
	return d.Logger
}

// documentUID reads the request ID at path from doc. The ID must be a
// non-empty string usable in a topic name.
func documentUID(doc datamodel.Document, path string) (string, error) {
	v, err := doc.GetField(path)
	if err != nil {
		return "", err
	}
	uid, ok := v.(string)
	if !ok || uid == "" {
		return "", fmt.Errorf("%s: expected a non-empty string, got %v", path, v)
	}
	if strings.ContainsAny(uid, "/ \t\n") {
		return "", fmt.Errorf("%s: invalid uid %q", path, uid)
	}
	return uid, nil
}
