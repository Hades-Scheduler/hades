---
title: Kubernetes (GitHub Actions)
sidebar_position: 4
---

# Deploying to Kubernetes with GitHub Actions

Hades ships two manual GitHub Actions workflows that install/upgrade the Helm chart on a
Kubernetes cluster. The default setup targets **one cluster with two namespaces**:
`hades` (production) and `hades-test` (test).

| Workflow | Environment | Namespace | Version rule |
|---|---|---|---|
| **Deploy to Kubernetes (prod)** | `k8s-prod` | `hades` | `latest` or a **published GitHub Release** tag (validated) |
| **Deploy to Kubernetes (test)** | `k8s-test` | `hades-test` | **any** image tag (`latest`, a release tag, `pr-N`, a branch/sha tag) |

Both are `workflow_dispatch` only - nothing deploys automatically. They call the reusable
`deploy-k8s.yml`, which does the actual work.

## How a deploy runs

1. **Checkout** the chart source. The chart templates and CRDs come from this ref, so it
   must match the deployed image - especially for CRDs, which Helm never upgrades (see
   step 5). Prod checks out the **release tag** (when the version is not `latest`). Test
   resolves the ref automatically: an explicit `chart-ref` input wins; otherwise a `pr-N`
   version sources the chart/CRDs from that PR's head (`refs/pull/N/head`); otherwise it
   uses the ref the workflow was run from. This means deploying a `pr-N` image also
   deploys that PR's CRDs, even when the run is dispatched from `main`.
2. **Configure kubeconfig** from the environment's `KUBE_CONFIG` secret.
3. **Ensure the namespace** exists.
4. **Create/update the app Secrets** (`hades-auth`, `hades-dashboard`) from the
   environment secrets. The first deploy creates them; later deploys keep them in sync.
5. **Apply the CRDs** with a server-side apply. Helm never upgrades CRDs after the first
   install, and server-side apply avoids the client-side annotation size limit on the
   `BuildJob` CRD.
6. **`helm upgrade --install --atomic`** with the base `values.yaml` plus the
   environment values file, pinning all four component image tags to the chosen version.
   `--atomic` rolls the release back on failure.
7. **Roll all four Hades deployments** and wait for each rollout. This covers the two
   cases Helm does not roll on its own: a **mutable image tag** (`latest`, `pr-N`) whose
   digest changed but whose tag string did not (Helm sees an unchanged spec, so the
   restart re-pulls the new digest via `pullPolicy: Always`), and **rotated Secrets**
   (`AUTH_KEY`, dashboard credentials) that pods read as environment variables only at
   start. For an immutable release tag Helm already rolls, so the restart is a no-op.

The `version` input maps directly to the container image tag (this repo tags images with
the release tag verbatim, with no `v` prefix).

## Selecting the version

- **Test**: type any image tag. Handy tags: `latest` (current `main`), `pr-123` (a PR
  build), or a release tag. For a `pr-N` tag the chart and CRDs are taken from that PR's
  head automatically, so you can dispatch from any branch and still get the PR's CRDs. To
  source the chart from a specific branch or sha instead, set the optional `chart-ref`
  input.
- **Prod**: type `latest` or an existing release tag (for example `1.0.0`). A bogus value
  fails the `validate` job before the cluster is touched. The prod deploy pins the chart
  source automatically - `main` for `latest`, or the release tag otherwise - so the chart
  and CRDs match the images regardless of which ref the run was dispatched from.

## Per-environment configuration

Non-secret differences live in committed values files, layered on top of `values.yaml`:

- `helm/hades/values-prod.yaml` - prod ingress host and Secret wiring.
- `helm/hades/values-test.yaml` - test ingress host, Secret wiring, and the
  `nats.host` override for the `hades-test` namespace.

Only credentials are stored as GitHub secrets.

## Monitoring

Every Hades service exposes a Prometheus `/metrics` endpoint on a dedicated,
cluster-internal port (`8082` by default, `monitoring.port`). The endpoint is
**never** routed through the public ingress. It always carries Go runtime and
process metrics, plus a few domain counters (`hades_build_requests_total`,
`hades_jobs_enqueued_total`, `hades_jobs_scheduled_total`) and, for the operator,
controller-runtime reconcile/workqueue metrics.

Registering these with a cluster Prometheus uses a `ServiceMonitor`, which is
**off by default** so the chart installs cleanly whether or not the cluster runs
the [Prometheus Operator](https://github.com/prometheus-operator/prometheus-operator).
Enable it only once the operator's CRDs (`monitoring.coreos.com`) are present:

```yaml
# values-<env>.yaml
monitoring:
  enabled: true
  serviceMonitor:
    # Match the label your Prometheus uses to select ServiceMonitors.
    labels:
      release: kube-prometheus-stack
```

The rendered `ServiceMonitor` selects all Hades Services by
`app.kubernetes.io/part-of: hades` and scrapes their `metrics` port. Verify the
targets appear in Prometheus with `up{job=~"hades.*"} == 1`.

## Required secrets

Add these to **each** GitHub environment (`k8s-prod` and `k8s-test`):

| Secret | Purpose |
|---|---|
| `KUBE_CONFIG` | kubeconfig (plain YAML) for the target cluster |
| `AUTH_KEY` | protects the API `/build` endpoint (Secret `hades-auth`) |
| `DASHBOARD_USERNAME` | dashboard login user (Secret `hades-dashboard`) |
| `DASHBOARD_PASSWORD_HASH` | bcrypt hash of the dashboard password |
| `DASHBOARD_SESSION_SECRET` | dashboard session signing secret (>=32 chars) |

Paste the kubeconfig file's contents directly as `KUBE_CONFIG` (plain YAML, no encoding).

## Production approval

The `k8s-prod` environment uses a **required-reviewer** protection rule, so a production
deploy pauses for manual approval before it runs. TLS certificates are issued in-cluster
by cert-manager (the `letsencrypt-prod` cluster-issuer), so no ACME secrets are needed in
the workflow.

## Troubleshooting

When a deploy fails, the reusable workflow prints pods, events, and the logs/description
of any not-ready pod. Because `--atomic` rolls back (and deletes the release on a failed
first install), check that diagnostics step in the run log for the real cause (commonly
`ImagePullBackOff` for a nonexistent tag, or a missing secret).
