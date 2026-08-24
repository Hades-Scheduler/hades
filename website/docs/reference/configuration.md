---
sidebar_position: 2
title: Configuration Reference
---

# Configuration Reference

Hades is configured entirely through environment variables (loaded via `caarlos0/env`, plus an optional `.env` file for local runs). This is the single source of truth for every variable each component reads. A blank **Default** means the variable is optional and unset by default.

:::tip
`.env.example` at the repository root is a ready-to-copy template covering the common variables.
:::

## Global (all components)

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `DEBUG` | `false` | Set to `true` for verbose (debug-level) logging. Read by every binary. |

## NATS connection (all components)

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `NATS_URL` | `nats://localhost:4222` | NATS server URL. |
| `NATS_USERNAME` | | NATS username (optional). |
| `NATS_PASSWORD` | | NATS password (optional). |
| `NATS_TLS_ENABLED` | `false` | Enable TLS for the NATS connection. |

## Metrics (all components)

Every service exposes a Prometheus `/metrics` endpoint on a dedicated, cluster-internal port (`METRICS_PORT`, default `8082`; the operator uses the `--metrics-bind-address` flag). It always includes Go runtime and process collectors, plus a few domain counters (`hades_build_requests_total`, `hades_jobs_enqueued_total`, `hades_jobs_scheduled_total`) and, for the operator, controller-runtime reconcile/workqueue metrics. The port is never routed through the public ingress; enable scraping by a Prometheus Operator with `--set monitoring.enabled=true`.

## Overhead timing & tracing (all components)

Hades measures how much overhead it adds around a job, per step and per phase, classifying every phase as `runtime` (the user's container executing) or `overhead` (Hades/Kubernetes coordination). One `shared/timing.JobTimer` drives three sinks:

- **Logs** (always on): a debug-level event per phase and one info-level `job timing summary` per job (`overhead_ms`/`runtime_ms`/`wall_ms`/`overhead_pct`).
- **Prometheus** (same `/metrics` endpoint): `hades_phase_seconds{executor,phase,kind}`, `hades_image_pull_seconds{executor,cached}`, and rollups `hades_job_overhead_seconds` / `hades_job_runtime_seconds` / `hades_job_wall_seconds`.
- **OpenTelemetry traces** (opt-in): set `OTEL_EXPORTER_OTLP_ENDPOINT` to render each job as a waterfall across API → scheduler → operator. The trace context flows via the payload `traceparent` and the BuildJob `hades.tum.de/traceparent` annotation; unset leaves a zero-cost noop tracer.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | | OTLP gRPC endpoint (e.g. `http://jaeger:4317`). Unset disables tracing. |
| `OTEL_SERVICE_NAME` | per service | Overrides the service name shown in traces. |

`make run` / `make docker-run` ship a Jaeger backend (UI on `http://localhost:16686`); in Kubernetes use `--set tracing.enabled=true` with `tracing.endpoint` or `tracing.deployJaeger=true`. The bundled Jaeger UI is ClusterIP by default (`kubectl port-forward svc/hades-jaeger 16686:16686`); to expose it, `tracing.jaeger.ui.ingress.enabled=true` renders an Ingress that is **always behind HTTP basic auth** (set `tracing.jaeger.ui.auth.password` or `...auth.existingSecret`; the chart refuses to render it unauthenticated). Docker phases are millisecond-precise; Kubernetes step phases are second-granular (Kubernetes truncates container timestamps to whole seconds).

## HadesAPI

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `API_PORT` | `8080` | Port the HTTP API listens on. |
| `METRICS_PORT` | `8082` | Port the Prometheus `/metrics` endpoint listens on. |
| `AUTH_KEY` | | HTTP Basic Auth key for the `hades` user. Empty disables auth. |

:::note Reserved (not yet implemented)
`PROMETHEUS_ADDRESS`, `RETENTION_IN_MIN`, `MAX_RETRIES`, and `TIMEOUT_IN_MIN` appear in `.env.example` but are **not read by any component today**. They are placeholders for planned features. Prometheus metrics are served on `METRICS_PORT`, not `PROMETHEUS_ADDRESS`.
:::

## HadesScheduler

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `CONCURRENCY` | `1` | Number of jobs processed concurrently. |
| `NATS_ACK_WAIT` | `1m` | How long JetStream waits for an ack before redelivering a job to another worker. A liveness backstop, not a job-duration budget: a worker signals progress while a job runs, so long jobs are never redelivered while the worker is alive. Minimum `300ms`: the heartbeat targets `AckWait/3` but is clamped to a `100ms` floor, so shorter values leave fewer than three ticks per ack window and lose the margin that absorbs a delayed or dropped heartbeat. |
| `NATS_MAX_DELIVER` | `3` | Maximum number of deliveries per job. The last delivery is spent publishing a terminal `Failed` status and dropping the job, so the default allows two execution attempts. Must be at least `2` - the scheduler refuses to start otherwise. |
| `METRICS_PORT` | `8082` | Port the Prometheus `/metrics` endpoint listens on. |
| `HADES_EXECUTOR` | `docker` | Execution platform: `docker` or `k8s`. |

Depending on `HADES_EXECUTOR`, the Docker or Kubernetes variables below also apply.

### Docker executor (`HADES_EXECUTOR=docker`)

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `DOCKER_HOST` | `unix:///var/run/docker.sock` | Docker daemon endpoint. |
| `DOCKER_CONTAINER_AUTOREMOVE` | `false` | Auto-remove step containers after they exit. |
| `DOCKER_SCRIPT_EXECUTOR` | `/bin/bash -c` | Shell used to run each step's `script`. |
| `DOCKER_CPU_LIMIT` | | Default CPU limit (whole CPUs) when a step sets none. |
| `DOCKER_MEMORY_LIMIT` | | Default memory limit (e.g. `4g`) when a step sets none. |

### Kubernetes executor (`HADES_EXECUTOR=k8s`)

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `K8S_NAMESPACE` | `hades-executor` | Namespace jobs are scheduled into. |
| `BUILDJOB_GROUP` | `build.hades.tum.de` | API group of the `BuildJob` CRD. |
| `BUILDJOB_VERSION` | `v1` | API version of the `BuildJob` CRD. |
| `BUILDJOB_RESOURCE` | `buildjobs` | Plural resource name of the `BuildJob` CRD. |

## HadesOperator

Only deployed when the scheduler runs in `operator` mode.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `WATCH_NAMESPACE` | | Namespace the operator watches (empty = all namespaces, subject to RBAC). |
| `DELETE_ON_COMPLETE` | `true` | Delete the `BuildJob` CR (and its `Job`) once it finishes. |
| `MAX_PARALLELISM` | `100` | Maximum concurrent Jobs the operator admits; excess are suspended. |
| `REQUEUE_DELAY` | `2s` | How often the operator re-reconciles a running `BuildJob` (Go duration). Pods are not watched, so completion is detected on these requeues. A zero or negative duration falls back to the default; a value that is not a valid Go duration fails operator startup. |
| `LOG_DRAIN_TIMEOUT` | `45s` | How long a completed `BuildJob` is kept while its container logs drain, before it is deleted anyway (Go duration). A zero or negative duration falls back to the default; a value that is not a valid Go duration fails operator startup. |
| `DEV_MODE` | `false` | Enable the controller-runtime development logger. |

The operator also accepts standard controller-runtime flags: `--health-probe-bind-address` (default `:8083`), `--metrics-bind-address` (default `:8082`, set `0` to disable), `--leader-elect`, and the log flags.

## HadesLogManager

Deployed by the Helm chart (`hades-log-manager`); also run locally via `make run`.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HADESLOGMANAGER_API_PORT` | `8081` | HTTP API port. |
| `METRICS_PORT` | `8082` | Port the Prometheus `/metrics` endpoint listens on. |
| `LOG_BATCH_SIZE` | `100` | Log entries buffered before a flush. |
| `LOG_RETENTION` | `1h` | How long completed-job logs are kept in memory (Go duration). |
| `MAX_JOB_LOGS` | `1000` | Max log entries retained per job. |
| `STATUS_WEBHOOK_ENABLED` | `true` | Deliver the [job-status webhook](../usage/submitting-jobs#job-status-webhook). Inert for jobs without a `status_callback_url`. |
| `STATUS_WEBHOOK_MAX_ATTEMPTS` | `6` | Delivery attempts per job before the status event is dropped. |
| `STATUS_WEBHOOK_TIMEOUT` | `10s` | Bound on a single delivery, including the callback-URL lookup. |
| `STATUS_WEBHOOK_INITIAL_BACKOFF` | `5s` | Delay before the second attempt; doubles per attempt. |
| `STATUS_WEBHOOK_MAX_BACKOFF` | `5m` | Ceiling for the retry delay. |
| `STATUS_WEBHOOK_CONCURRENCY` | `16` | Deliveries in flight at once; keeps a dead receiver from delaying other jobs. |
| `STATUS_WEBHOOK_MAX_PENDING` | `1000` | Status events awaiting acknowledgement (in flight or backing off). |

Both outbound pushes are configured per job, not globally:

- `callback_url` - an absolute `http`/`https` URL with a host. The Log Manager forwards that job's aggregated **logs** there once the log stream has drained. If omitted, the job's logs are not forwarded.
- `status_callback_url` - an absolute `http`/`https` URL with a host. Receives the [job-status webhook](../usage/submitting-jobs#job-status-webhook) when the job reaches a terminal status, independently of log forwarding. If omitted, no webhook is sent.
