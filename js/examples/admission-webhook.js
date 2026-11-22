// admission-webhook.js -- Kubernetes-style admission webhooks as pipelines.
//
//   ./js/bin/dbsp js/examples/admission-webhook.js
//   curl -s -X POST -H 'content-type: application/json' http://127.0.0.1:8443/validate \
//     -d '{"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview","request":{"uid":"u1","namespace":"default"}}'
//   curl -s -X POST -H 'content-type: application/json' http://127.0.0.1:8443/validate \
//     -d '{"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview","request":{"uid":"u2","namespace":"forbidden"}}'
//   curl -s -X POST -H 'content-type: application/json' http://127.0.0.1:8443/mutate \
//     -d '{"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview","request":{"uid":"u3","namespace":"default"}}'
//
// One webhook server routes each HTTP path to its own topic, so each
// circuit sees only its own requests: /validate feeds a validating
// pipeline that denies the "forbidden" namespace, /mutate a mutating one
// that answers with a JSONPatch adding an annotation. Both answer through
// the "responses" topic; the webhook target routes each response back to
// the waiting HTTP request by its uid. Plain HTTP for the demo; set
// certFile/keyFile for a real cluster, which requires HTTPS. Stop it with
// Ctrl-C.

webhook.serve({
  address: "127.0.0.1:8443",
  routes: { "/validate": "validate-requests", "/mutate": "mutate-requests" },
});

aggregate
  .compile(
    [
      {
        "@project": {
          apiVersion: "admission.k8s.io/v1",
          kind: "AdmissionReview",
          response: {
            uid: "$.request.uid",
            warnings: ["warning from wh server"],
            allowed: { "@neq": ["$.request.namespace", "forbidden"] },
          },
        },
      },
    ],
    { name: "validate", inputs: "validate-requests", outputs: ["responses"] },
  )
  .commit();

// base64 of [{"op":"add","path":"/metadata/annotations","value":{"dbsp/mutated":"true"}}]
const PATCH =
  "W3sib3AiOiJhZGQiLCJwYXRoIjoiL21ldGFkYXRhL2Fubm90YXRpb25zIiwidmFsdWUiOnsiZGJzcC9tdXRhdGVkIjoidHJ1ZSJ9fV0=";

aggregate
  .compile(
    [
      {
        "@project": {
          apiVersion: "admission.k8s.io/v1",
          kind: "AdmissionReview",
          response: {
            uid: "$.request.uid",
            allowed: true,
            patchType: "JSONPatch",
            patch: PATCH,
          },
        },
      },
    ],
    { name: "mutate", inputs: "mutate-requests", outputs: ["responses"] },
  )
  .commit();

webhook.respond("responses", {});

console.log("Admission webhooks on http://127.0.0.1:8443/{validate,mutate}");
console.log(webhook.toJSON())