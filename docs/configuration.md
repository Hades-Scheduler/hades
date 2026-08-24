# Configuration Reference

Hades is configured entirely through environment variables (loaded via `caarlos0/env` and, for local runs, an optional `.env` file read by `joho/godotenv`). This page is the single source of truth for every variable each component reads; component READMEs and the top-level `Readme.md` link here instead of duplicating tables.

Each table is derived directly from the `Config` structs in the code, cited per section. A blank **Default** means the variable is optional and unset by default.

> **Tip:** `.env.example` at the repository root is a ready-to-copy template covering the common variables.

## Global (all components)

| Variable | Default | Description | Source |
| -------- | ------- | ----------- | ------ |
| `DEBUG` | `false` | Set to `true` for verbose (debug-level) logging. Read by every binary. | `shared/utils/logging.go` |

## NATS connection (all components)

Every component connects to NATS with the same `ConnectionConfig`.

| Variable | Default | Description | Source |
| -------- | ------- | ----------- | ------ |
| `NATS_URL` | `nats://localhost:4222` | NATS server URL. | `shared/nats/connection.go` |
| `NATS_USERNAME` | | NATS username (optional). | `shared/nats/connection.go` |
| `NATS_PASSWORD` | | NATS password (optional). | `shared/nats/connection.go` |
| `NATS_TLS_ENABLED` | `false` | Enable TLS for the NATS connection. | `shared/nats/connection.go` |

## Metrics (all components)

Every service exposes a Prometheus `/metrics` endpoint on a dedicated, cluster-internal port (`METRICS_PORT`, default `8082`; the operator uses the `--metrics-bind-address` flag). The endpoint always includes Go runtime and process collectors; the API and scheduler add a few domain counters (`hades_build_requests_total`, `hades_jobs_enqueued_total`, `hades_jobs_scheduled_total`), and the operator adds controller-runtime reconcile/workqueue metrics.

The metrics port is never routed through the public ingress. In Kubernetes, enable scraping by a Prometheus Operator with `--set monitoring.enabled=true` (see the [Kubernetes deployment guide](./deployment/kubernetes-github-actions.md#monitoring)). The `shared/metrics` package serves the endpoint (`shared/metrics/metrics.go`).

## Overhead timing & tracing (all components)

Hades instruments how much overhead it adds around a job, broken down per step and per phase, so you can answer "what fraction of wall-clock was Hades coordination versus the user's container actually running". A single `shared/timing.JobTimer` drives three sinks from the same measurement seams.

**Phase taxonomy.** Each phase is classified as `runtime` (the user's container executing) or `overhead` (everything Hades/Kubernetes does around it):

| Phase | Kind | Executor | Meaning |
| ----- | ---- | -------- | ------- |
| `queue_wait` | overhead | both | API submission → scheduler starts handling the job |
| `provision` | overhead | both | setup before the first container runs (Docker: volume create; K8s: CR create → first step starts) |
| `image_pull` | overhead | docker | pulling the step image (labelled `cached` for warm vs cold pulls) |
| `container_create` | overhead | docker | `ContainerCreate` |
| `container_startup` | overhead | docker | `ContainerStart` |
| `container_run` | **runtime** | docker | the container process running until it exits |
| `log_drain` | overhead | docker | flushing the container's final logs after exit |
| `container_remove` | overhead | docker | container cleanup |
| `step_wait` | overhead | k8s | scheduling + image pull before a step's container starts |
| `step_run` | **runtime** | k8s | the step container running (from pod timestamps) |
| `reconcile_detection_lag` | overhead | k8s | last step finishing → the operator observing completion (dominated by the 2 s requeue poll) |

Per-job rollups are logged and exported: `overhead_total`, `runtime_total`, `wall_total`, and `overhead_pct = overhead / (overhead + runtime)`.

**1. Structured logs (always on).** Each phase emits an slog event at debug level (`phase`, `kind`, `executor`, `job_id`, `step`, `dur_ms`); each job emits one info-level `job timing summary` line with `overhead_ms`/`runtime_ms`/`wall_ms`/`overhead_pct`. Set `DEBUG=true` to see per-phase lines.

**2. Prometheus histograms.** On the same `/metrics` endpoint as the counters above: `hades_phase_seconds{executor,phase,kind}`, `hades_image_pull_seconds{executor,cached}`, and the rollups `hades_job_overhead_seconds{executor}`, `hades_job_runtime_seconds{executor}`, `hades_job_wall_seconds{executor}`. Buckets span 1 ms–1 h.

**3. OpenTelemetry traces (opt-in).** When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, every service exports spans so a job renders as a per-job waterfall across API → scheduler → operator. The trace context is propagated from the API through NATS (the job payload's `traceparent`) and into Kubernetes (the BuildJob's `hades.tum.de/traceparent` annotation); the operator emits backdated step spans from the pod's container timestamps. With the endpoint unset, a noop tracer runs and tracing costs nothing.

| Variable | Default | Description | Source |
| -------- | ------- | ----------- | ------ |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | | OTLP gRPC endpoint spans are exported to (e.g. `http://jaeger:4317`). Unset disables tracing. | `shared/timing/tracing.go` |
| `OTEL_SERVICE_NAME` | per service | Overrides the service name shown in traces. | `shared/timing/tracing.go` |

The local stacks ship a working backend: `make run` and `make docker-run` start a Jaeger all-in-one (UI on <http://localhost:16686>) and point the services at it. In Kubernetes, enable it with `--set tracing.enabled=true` and either `--set tracing.endpoint=<your-collector>:4317` or `--set tracing.deployJaeger=true` (bundled Jaeger, dev/test only).

The bundled Jaeger UI Service is cluster-internal (ClusterIP) by default, so you reach it with `kubectl port-forward svc/hades-jaeger 16686:16686`. To expose it outside the cluster, enable its Ingress - which is **always protected by HTTP basic auth**: set `tracing.jaeger.ui.ingress.enabled=true`, a `host`, and either `tracing.jaeger.ui.auth.password` (the chart hashes it into an htpasswd Secret) or `tracing.jaeger.ui.auth.existingSecret`. The chart refuses to render the UI Ingress without credentials, so the UI can never be exposed unauthenticated. Basic auth is implemented with nginx-ingress annotations (`tracing.jaeger.ui.ingress.className` defaults to `nginx`).

**Accuracy notes.** Docker phases use a single monotonic clock, so they are millisecond-precise and tile the timeline (`overhead + runtime = wall`). Kubernetes records container timestamps to whole-second precision, so K8s per-step phases are second-granular; `queue_wait` crosses hosts, so a skewed clock is clamped to zero.

## HadesAPI

| Variable | Default | Description | Source |
| -------- | ------- | ----------- | ------ |
| `API_PORT` | `8080` | Port the HTTP API listens on. | `HadesAPI/main.go` (`HadesAPIConfig`) |
| `METRICS_PORT` | `8082` | Port the Prometheus `/metrics` endpoint listens on (see [Metrics](#metrics-all-components)). | `HadesAPI/main.go` (`HadesAPIConfig`) |
| `AUTH_KEY` | | HTTP Basic Auth key for the `hades` user. Empty disables auth (a warning is logged). | `HadesAPI/main.go` (`HadesAPIConfig`) |

Plus the [NATS](#nats-connection-all-components) and [global](#global-all-components) variables.

> **Reserved (not yet implemented):** `PROMETHEUS_ADDRESS`, `RETENTION_IN_MIN`, `MAX_RETRIES`, and `TIMEOUT_IN_MIN` appear in `.env.example` and some compose files but are **not read by any component today**. They are placeholders for planned features and currently have no effect. (Note: a per-job timeout *is* supported, but it is set on the job payload as `timeout_seconds`, not via this env var - see [API: `Step`/`QueuePayload`](./api.md#queuepayload).)
>
> **Per-job/per-step resource controls** are set on the job payload, not via environment variables: `timeout_seconds` (job), and per-step `cpu_limit`, `memory_limit`, `network`, `memory_swap`, `pids_limit`. Timeout, CPU, memory and environment variables are enforced on both executors; `network`, `memory_swap` and `pids_limit` are Docker-executor only (Kubernetes has no per-container swap/PID field, and a pod's containers share one network namespace). See [api.md](./api.md#step).

## HadesScheduler

| Variable | Default | Description | Source |
| -------- | ------- | ----------- | ------ |
| `CONCURRENCY` | `1` | Number of jobs processed concurrently. | `HadesScheduler/main.go` (`HadesSchedulerConfig`) |
| `METRICS_PORT` | `8082` | Port the Prometheus `/metrics` endpoint listens on (see [Metrics](#metrics-all-components)). | `HadesScheduler/main.go` (`HadesSchedulerConfig`) |
| `HADES_EXECUTOR` | `docker` | Execution platform: `docker` or `k8s`. | `shared/utils/config.go` (`ExecutorConfig`) |

Plus the [NATS](#nats-connection-all-components) and [global](#global-all-components) variables, and - depending on `HADES_EXECUTOR` - the Docker or Kubernetes variables below.

### Docker executor (`HADES_EXECUTOR=docker`)

| Variable | Default | Description | Source |
| -------- | ------- | ----------- | ------ |
| `DOCKER_HOST` | `unix:///var/run/docker.sock` | Docker daemon endpoint. | `HadesScheduler/docker/env.go` (`EnvConfig`) |
| `DOCKER_CONTAINER_AUTOREMOVE` | `false` | Auto-remove step containers after they exit. Keep `false` to retain logs post-run. | `HadesScheduler/docker/env.go` |
| `DOCKER_SCRIPT_EXECUTOR` | `/bin/bash -c` | Shell used to run each step's `script`. | `HadesScheduler/docker/env.go` |
| `DOCKER_CPU_LIMIT` | | Default CPU limit (whole CPUs, e.g. `6`) when a step sets none. | `HadesScheduler/docker/env.go` |
| `DOCKER_MEMORY_LIMIT` | | Default memory limit (e.g. `4g`) when a step sets none. | `HadesScheduler/docker/env.go` |

### Kubernetes executor (`HADES_EXECUTOR=k8s`)

| Variable | Default | Description | Source |
| -------- | ------- | ----------- | ------ |
| `K8S_NAMESPACE` | `hades-executor` | Namespace jobs are scheduled into. | `HadesScheduler/k8s/k8s.go` |
| `BUILDJOB_GROUP` | `build.hades.tum.de` | API group of the `BuildJob` CRD. | `HadesScheduler/k8s/k8s.go` (`BuildJobGVRConfig`) |
| `BUILDJOB_VERSION` | `v1` | API version of the `BuildJob` CRD. | `HadesScheduler/k8s/k8s.go` |
| `BUILDJOB_RESOURCE` | `buildjobs` | Plural resource name of the `BuildJob` CRD. | `HadesScheduler/k8s/k8s.go` |

## HadesOperator

Only deployed when the scheduler runs in `operator` mode.

| Variable | Default | Description | Source |
| -------- | ------- | ----------- | ------ |
| `WATCH_NAMESPACE` | | Namespace the operator watches for `BuildJob` CRs. Empty means all namespaces (subject to RBAC). | `HadesScheduler/HadesOperator/cmd/main.go` (`NSConfig`) |
| `DELETE_ON_COMPLETE` | `true` | Delete the `BuildJob` CR (and its `Job`) once it finishes. Set `false` to retain them for debugging. | `HadesScheduler/HadesOperator/cmd/main.go` (`OperatorConfig`) |
| `MAX_PARALLELISM` | `100` | Maximum number of `Job`s the operator admits concurrently; excess jobs are suspended. | `HadesScheduler/HadesOperator/cmd/main.go` (`OperatorConfig`) |
| `REQUEUE_DELAY` | `2s` | How often the operator re-reconciles a running `BuildJob` (Go duration). The operator does not watch Pods, so container transitions and job completion are only observed on these requeues; lower it for faster completion detection at the cost of more API traffic. A zero or negative duration falls back to the default; a value that is not a valid Go duration fails operator startup. | `HadesScheduler/HadesOperator/cmd/main.go` (`OperatorConfig`) |
| `LOG_DRAIN_TIMEOUT` | `45s` | How long a completed `BuildJob` is kept while its container logs drain, before it is deleted anyway (Go duration). Only relevant with `DELETE_ON_COMPLETE=true`. A zero or negative duration falls back to the default; a value that is not a valid Go duration fails operator startup. | `HadesScheduler/HadesOperator/cmd/main.go` (`OperatorConfig`) |
| `DEV_MODE` | `false` | Enable the controller-runtime development logger. | `HadesScheduler/HadesOperator/cmd/main.go` |

Plus the [NATS](#nats-connection-all-components) variables (the operator publishes status/log events). The operator also accepts standard controller-runtime **flags**: `--health-probe-bind-address` (default `:8083`), `--metrics-bind-address` (default `:8082`, set `0` to disable), `--leader-elect`, and the log flags bound via `opts.BindFlags`.

## HadesLogManager

Deployed by the Helm chart (`hades-log-manager`); also run locally via `make run`.

| Variable | Default | Description | Source |
| -------- | ------- | ----------- | ------ |
| `HADESLOGMANAGER_API_PORT` | `8081` | HTTP API port. | `HadesLogManager/main.go` (`HadesLogManagerConfig`) |
| `METRICS_PORT` | `8082` | Port the Prometheus `/metrics` endpoint listens on (see [Metrics](#metrics-all-components)). | `HadesLogManager/main.go` (`HadesLogManagerConfig`) |
| `LOG_BATCH_SIZE` | `100` | Log entries buffered before a flush. | `HadesLogManager/processor.go` (`AggregatorConfig`) |
| `LOG_RETENTION` | `1h` | How long completed-job logs are kept in memory (Go duration). | `HadesLogManager/processor.go` |
| `MAX_JOB_LOGS` | `1000` | Max log entries retained per job. | `HadesLogManager/processor.go` |
| `STATUS_WEBHOOK_ENABLED` | `true` | Deliver the job-status webhook. Inert for jobs without a `status_callback_url`. | `HadesLogManager/status_webhook.go` (`StatusWebhookConfig`) |
| `STATUS_WEBHOOK_MAX_ATTEMPTS` | `6` | Delivery attempts per job before the status event is dropped. | `HadesLogManager/status_webhook.go` |
| `STATUS_WEBHOOK_TIMEOUT` | `10s` | Bound on a single delivery, including the callback-URL lookup. | `HadesLogManager/status_webhook.go` |
| `STATUS_WEBHOOK_INITIAL_BACKOFF` | `5s` | Delay before the second attempt; doubles per attempt. | `HadesLogManager/status_webhook.go` |
| `STATUS_WEBHOOK_MAX_BACKOFF` | `5m` | Ceiling for the retry delay. | `HadesLogManager/status_webhook.go` |
| `STATUS_WEBHOOK_CONCURRENCY` | `16` | Deliveries in flight at once; keeps a dead receiver from delaying other jobs. | `HadesLogManager/status_webhook.go` |
| `STATUS_WEBHOOK_MAX_PENDING` | `1000` | Status events awaiting acknowledgement (in flight or backing off). | `HadesLogManager/status_webhook.go` |

Both outbound pushes are configured per job, not globally:

- `callback_url` - an absolute `http`/`https` URL with a host. The Log Manager forwards that job's aggregated **logs** there once the log stream has drained. If omitted, the job's logs are not forwarded.
- `status_callback_url` - an absolute `http`/`https` URL with a host. Receives the **job-status webhook** on the terminal status, independently of log forwarding. See [`HadesLogManager/Readme.md`](../HadesLogManager/Readme.md#job-status-webhook).

Plus the [NATS](#nats-connection-all-components) and [global](#global-all-components) variables.
