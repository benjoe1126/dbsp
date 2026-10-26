package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// admissionReview builds an AdmissionReview request body.
func admissionReview(uid string) []byte {
	b, _ := json.Marshal(map[string]any{
		"apiVersion": "admission.k8s.io/v1",
		"kind":       "AdmissionReview",
		"request":    map[string]any{"uid": uid, "operation": "CREATE"},
	})
	return b
}

// responder stands in for the circuit: it answers each inserted request
// on the response topic, allowing it unless deny is set, and records the
// weights it saw per uid.
func responder(rt *dbspruntime.Runtime, in, out string, deny bool, seen chan<- [2]any) {
	sub := rt.NewSubscriber()
	sub.Subscribe(in)
	pub := rt.NewPublisher()
	go func() {
		for {
			evt, ok := sub.Next()
			if !ok {
				return
			}
			evt.Data.Iter(func(doc datamodel.Document, w zset.Weight) bool {
				uid, _ := doc.GetField("$.request.uid")
				seen <- [2]any{uid, w}
				resp := zset.New()
				resp.Insert(dbspunstructured.New(map[string]any{
					"apiVersion": "admission.k8s.io/v1",
					"kind":       "AdmissionReview",
					"response":   map[string]any{"uid": uid, "allowed": !deny},
				}), w)
				_ = pub.Publish(dbspruntime.Event{Name: out, Data: resp})
				return true
			})
		}
	}()
}

var _ = Describe("Webhook connector", func() {
	var (
		rt     *dbspruntime.Runtime
		ctx    context.Context
		cancel context.CancelFunc
		seen   chan [2]any
	)

	start := func(spec ProducerSpec, withResponder bool) string {
		seen = make(chan [2]any, 16)
		if withResponder {
			responder(rt, "requests", "responses", false, seen)
		}
		p, err := NewProducer("requests", spec, Deps{Runtime: rt})
		Expect(err).NotTo(HaveOccurred())
		c, err := NewConsumer("responses", ConsumerSpec{}, Deps{Runtime: rt})
		Expect(err).NotTo(HaveOccurred())
		Expect(rt.Add(p)).To(Succeed())
		Expect(rt.Add(c)).To(Succeed())
		go func() { _ = rt.Start(ctx) }()
		addr := p.Addr(ctx)
		Expect(addr).NotTo(BeNil())
		return "http://" + addr.String() + "/"
	}

	post := func(url string, body []byte) (int, map[string]any) {
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		Expect(err).NotTo(HaveOccurred())
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		return resp.StatusCode, out
	}

	BeforeEach(func() {
		rt = dbspruntime.NewRuntime("test", logr.Discard())
		ctx, cancel = context.WithCancel(context.Background())
	})

	AfterEach(func() { cancel() })

	It("answers a request with the response routed by its uid", func() {
		url := start(ProducerSpec{Address: "127.0.0.1:0"}, true)

		status, out := post(url, admissionReview("uid-1"))
		Expect(status).To(Equal(http.StatusOK))
		Expect(out).To(HaveKeyWithValue("response", map[string]any{"uid": "uid-1", "allowed": true}))

		Eventually(seen).Should(Receive(Equal([2]any{"uid-1", zset.Weight(1)})))
		// The request is retracted once answered.
		Eventually(seen).Should(Receive(Equal([2]any{"uid-1", zset.Weight(-1)})))
	})

	It("answers concurrent requests each with its own response", func() {
		url := start(ProducerSpec{Address: "127.0.0.1:0"}, true)

		uids := []string{"a", "b", "c", "d", "e"}
		got := make(chan string, len(uids))
		for _, uid := range uids {
			go func() {
				defer GinkgoRecover()
				status, out := post(url, admissionReview(uid))
				Expect(status).To(Equal(http.StatusOK))
				got <- out["response"].(map[string]any)["uid"].(string)
			}()
		}
		var answered []string
		for range uids {
			Eventually(got).Should(Receive(Satisfy(func(uid string) bool {
				answered = append(answered, uid)
				return true
			})))
		}
		Expect(answered).To(ConsistOf(uids))
	})

	It("rejects a request without a uid", func() {
		url := start(ProducerSpec{Address: "127.0.0.1:0"}, true)
		status, _ := post(url, []byte(`{"kind":"AdmissionReview"}`))
		Expect(status).To(Equal(http.StatusBadRequest))
	})

	It("times out and retracts a request that gets no response", func() {
		seenReq := make(chan [2]any, 4)
		sub := rt.NewSubscriber()
		sub.Subscribe("requests")
		go func() {
			for {
				evt, ok := sub.Next()
				if !ok {
					return
				}
				evt.Data.Iter(func(doc datamodel.Document, w zset.Weight) bool {
					uid, _ := doc.GetField("$.request.uid")
					seenReq <- [2]any{uid, w}
					return true
				})
			}
		}()
		DeferCleanup(sub.UnsubscribeAll)

		url := start(ProducerSpec{Address: "127.0.0.1:0", Timeout: "100ms"}, false)
		status, _ := post(url, admissionReview("lost"))
		Expect(status).To(Equal(http.StatusGatewayTimeout))
		Eventually(seenReq).Should(Receive(Equal([2]any{"lost", zset.Weight(1)})))
		Eventually(seenReq, time.Second).Should(Receive(Equal([2]any{"lost", zset.Weight(-1)})))
	})
})
