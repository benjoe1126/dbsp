// admission-webhook-operator.js -- Freeze the admission webhooks into a
// dcontroller Operator.
//
//   ./js/bin/dbsp js/examples/admission-webhook-operator.js [file] [--run]
//
// The declarative twin of admission-webhook.js: the same two circuits, but
// built as a RuntimeSpec (sources, circuits, targets) instead of with
// webhook.serve / aggregate.compile / webhook.respond. The spec is wrapped
// into an Operator CR and written as YAML (default:
// admission-webhook-operator.yaml), ready for `kubectl apply` against a
// dcontroller that includes the webhook connector.
//
// With --run the spec is also loaded into a local runtime, minus TLS, the
// same way dcontroller loads it, so the curl commands of
// admission-webhook.js work against it. Stop it with Ctrl-C.
//
// In a cluster the server needs a Service pointing at the dcontroller pod's
// port 8443, the TLS key pair mounted at /certs, and a Validating- and a
// MutatingWebhookConfiguration pointing at /validate and /mutate.

const fs = require("fs");

const run = argv.includes("--run");
const file = argv.find((a) => a !== "--run") || "admission-webhook-operator.yaml";

const WEBHOOK = "webhook.connector.dcontroller.io";

// base64 of [{"op":"add","path":"/metadata/annotations","value":{"dbsp/mutated":"true"}}]
const PATCH =
  "W3sib3AiOiJhZGQiLCJwYXRoIjoiL21ldGFkYXRhL2Fubm90YXRpb25zIiwidmFsdWUiOnsiZGJzcC9tdXRhdGVkIjoidHJ1ZSJ9fV0=";

const spec = {
  // One server: ValidateRequest is served on path, MutateRequest on its
  // route. A second webhook source on the same address would fail to listen.
  sources: [{
    apiGroup: WEBHOOK,
    kind: "ValidateRequest",
    parameters: {
      address: ":8443",
      path: "/validate",
      routes: { "/mutate": "MutateRequest" },
      certFile: "/certs/tls.crt",
      keyFile: "/certs/tls.key",
    },
  }],
  circuits: [
    {
      name: "validate",
      inputs: ["ValidateRequest"],
      outputs: ["AdmissionResponse"],
      pipeline: [{
        "@project": {
          apiVersion: "admission.k8s.io/v1",
          kind: "AdmissionReview",
          response: {
            uid: "$.request.uid",
            warnings: ["warning from wh server"],
            allowed: { "@neq": ["$.request.namespace", "forbidden"] },
          },
        },
      }],
      transforms: [{ name: "Incrementalizer" }],
    },
    {
      name: "mutate",
      inputs: ["MutateRequest"],
      outputs: ["AdmissionResponse"],
      pipeline: [{
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
      }],
      transforms: [{ name: "Incrementalizer" }],
    },
  ],
  // The responses of both circuits go back through one target.
  targets: [{ apiGroup: WEBHOOK, kind: "AdmissionResponse" }],
};

const operator = {
  apiVersion: "dcontroller.io/v1alpha1",
  kind: "Operator",
  metadata: { name: "admission-webhook" },
  spec,
};

const [[{ line }]] = format.toYAML([[operator, 1]]);
fs.writeFileSync(file, line);
console.log(`wrote the Operator to ${file}`);

if (run) {
  // Local run: plain HTTP on localhost, otherwise the very same spec.
  const local = JSON.parse(JSON.stringify(spec));
  const params = local.sources[0].parameters;
  delete params.certFile;
  delete params.keyFile;
  params.address = "127.0.0.1:8443";

  runtime.create("admission-webhook", local).start();
  console.log("Admission webhooks on http://127.0.0.1:8443/{validate,mutate}");
} else {
  exit();
}
