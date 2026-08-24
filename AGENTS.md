# AGENTS.md

Quick orientation for AI coding agents working on Hades. Pair this with `Readme.md` for user-facing docs; this file focuses on internal layout and gotchas.

## What this repo is

Hades is a job scheduler for containerized CI workloads (originally for programming-exercise grading at TUM). A user POSTs a multi-step job to `HadesAPI`, the API enqueues it on NATS JetStream, and `HadesScheduler` consumes it and runs each step in a container - either via the Docker daemon (local dev) or Kubernetes (production, via the `HadesOperator` and a `BuildJob` CRD). `HadesLogManager` is an in-memory aggregator that subscribes to log/status events on NATS and exposes them over HTTP.

## Repo layout

Go workspace (`go.work`, Go 1.26) with five modules:

| Module                              | Binary / role                                                                                                                                                                       |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `HadesAPI/`                         | Gin HTTP server. `POST /build` validates a payload, assigns a UUID, publishes to NATS by priority (and now also publishes `hades.jobstatus.Queued`). `GET /ping` health check. Optional Basic Auth via `AUTH_KEY`. Also hosts the optional **dashboard** (`HadesAPI/dashboard/` + embedded SPA in `HadesAPI/web/`).            |
| `HadesScheduler/`                   | NATS consumer. Reads `HADES_EXECUTOR` and dispatches to either the `docker/` or `k8s/` package.                                                                                     |
| `HadesScheduler/docker/`            | Runs each step as a Docker container; shares state between steps via a per-job named volume `shared-<uuid>`.                                                                        |
| `HadesScheduler/k8s/`               | Creates a `BuildJob` CR via the dynamic client for the operator to reconcile. |
| `HadesScheduler/HadesOperator/`     | Standalone kubebuilder operator. Watches `BuildJob` CRs (`build.hades.tum.de/v1`) and reconciles them into `batchv1.Job`s with one initContainer per step plus a finalizer pod.     |
| `HadesLogManager/`                  | Subscribes to `hades.jobstatus.*` and `hades.logs.<jobID>` on NATS, aggregates logs in-memory (`sync.Map`), exposes `GET /jobs`, `/jobs/:id/logs`, `/jobs/:id/status` on port 8081. Also hosts the **job-status webhook** dispatcher (`status_webhook.go` + `status_dispatcher.go`), which is independent of log forwarding.      |
| `shared/`                           | Cross-module: `payload` (DTOs), `nats` (publisher/consumer/connection), `buildlogs` (log types + `LogPublisher`/`LogAggregator` interfaces), `buildstatus` (job status enum + subjects), `redact` (metadata secret masking used by the API + dashboard), `utils` (env config loader, memory-limit parsing), `prio.go` (priority enum: high/medium/low ←→ ints). |

## Key contracts

- **Queue subjects:** `hades.jobs.{high,medium,low}` (priority-bucketed JetStream subjects).
- **Status subjects:** `hades.jobstatus.{Queued,Running,Succeeded,Failed,Stopped}` (payload = job ID string, optional `X-Hades-Reason` header). `HadesAPI` publishes `Queued` on enqueue; the scheduler/operator publish `Running`/`Succeeded`/`Failed`. Nothing publishes `Stopped` today. `HadesLogManager` and the dashboard subscribe to the whole `hades.jobstatus.*` lifecycle over **core NATS**.
- **Status stream:** `HADES_JOB_STATUS` is a JetStream stream over `hades.jobstatus.*` that durably captures those same core-NATS publishes. `HadesLogManager` consumes it through the durable `HADES_STATUS_WEBHOOK` consumer to deliver the **job-status webhook** (`status_callback_url`). Publishers are unchanged and core subscribers still receive every event.
- **Log subjects:** see `shared/buildlogs/buildlog_stream.go`.
- **Job DTO:** `shared/payload/payload.go` (`QueuePayload` with ordered `Step`s). Job-level: `timeout_seconds` (whole-job timeout). Each step has `image`, optional `script`, `metadata` env vars, `cpu_limit` (whole cores), `memory_limit`, and the Docker-only limits `network`, `memory_swap`, `pids_limit`.
  - **Two outbound pushes, do not conflate them:** `callback_url` forwards the aggregated **log array** and only after `stopWatchingJobLogs` drains the JetStream log consumer; `status_callback_url` receives the **job-status webhook** (`{event, job_id, name, status, reason, queued_at, started_at, finished_at, duration_ms, attempt}`) on the terminal status itself, with at-least-once retry. Neither may be repurposed as the other.
  - **Executor parity:** `timeout_seconds`, env (`metadata`), `cpu_limit`, `memory_limit` are enforced on both executors (Kubernetes: per-container resources + Job `activeDeadlineSeconds`). `network`, `memory_swap`, `pids_limit` are enforced on the **Docker executor only**; the operator accepts them for schema parity but does not apply them (pod containers share a network namespace; no per-container swap/PID field).
- **CRD:** `BuildJob` in `HadesScheduler/HadesOperator/api/v1/buildjob_types.go`. **Important:** if you change `BuildJobSpec`, run `make -C HadesScheduler/HadesOperator manifests generate` and commit `helm/hades/crds/build.hades.tum.de_buildjobs.yaml` and `zz_generated.deepcopy.go` - the `verify-crd.yml` GitHub workflow will fail otherwise. The `BuildJobSpec` is intentionally duplicated from `shared/payload`; keep them in sync manually.

## Build / test

```fish
# whole workspace
go build ./...
go test ./...
go work sync

# operator (kubebuilder targets)
make -C HadesScheduler/HadesOperator manifests generate fmt vet test

# helm chart
helm dependency build ./helm/hades
helm lint ./helm/hades
helm template hades ./helm/hades -n hades

# regenerate OpenAPI specs after changing a handler annotation or a request/response DTO
make docs-api
```

Both `HadesAPI/docs/` and `HadesLogManager/docs/` are generated (committed) from swaggo annotations; each Swagger UI is served at `/swagger/index.html` only when `DEBUG=true`. Run `make docs-api` after changing a handler annotation or a request/response DTO.

The Helm chart's values table in `helm/hades/Readme.md` is generated by `helm-docs` from the `# --` comments in `helm/hades/values.yaml`; edit prose in `helm/hades/Readme.md.gotmpl` (not the generated `Readme.md`) and run `make docs-helm`.

CI (`.github/workflows/ci.yml`) runs `lint` (`make lint` + `gofmt`), `build` (`make build`), a `ui` job (typecheck + `vitest` + build of `HadesAPI/web`), and a `test` matrix over `shared`, `HadesAPI`, `HadesScheduler`, `HadesLogManager`, and `HadesScheduler/HadesOperator`. It then builds and pushes Docker images for `hades-api`, `hades-scheduler`, `hades-operator`, and `hades-log-manager` to `ghcr.io/hades-scheduler/hades/`. `HadesLogManager` has a `Dockerfile`, a Helm deployment (ClusterIP service `hades-log-manager-service:8081`, single-replica), and a CI image build; it is **not** on the ingress and is reached only internally (e.g. by the dashboard's authenticated logs proxy). It is not in the `compose.yml` stack, so `make docker-run` does not start it (use `make run` locally).

The `hades-api` image is a **multi-stage** build: a Node stage builds the dashboard SPA (`HadesAPI/web`) and the Go stage embeds `HadesAPI/web/dist` via `//go:embed`. A placeholder `dist/index.html` is committed so `go build`/`go run` work before a UI build; `make ui-build` produces the real assets (git-ignored).

## Running locally

The top-level `Makefile` wraps the common workflows (`make help` lists every target):

- **CLI mode:** `make run` runs `HadesAPI`, `HadesScheduler`, and `HadesLogManager` via `go run` and auto-starts NATS in Docker.
- **Docker mode:** `make docker-run` brings up `hadesAPI` (8081→8080), `hadesScheduler` (docker executor), and `nats` via `compose.yml`. API requests: see `bruno/api/*.bru`.
- **K8s mode:** `helm upgrade --install hades ./helm/hades -n hades --create-namespace`. The chart deploys API, scheduler, operator, log manager, and embedded NATS JetStream.

## Conventions

- Logging: `log/slog` everywhere. Set `DEBUG=true` for debug level. Operator uses `sigs.k8s.io/controller-runtime` `zap` logger.
- Config: `caarlos0/env/v11` + `joho/godotenv`. Each binary has its own `Config` struct that calls `utils.LoadConfig`. Field tags must be exported; reflection silently ignores lowercase fields.
- Errors: prefer wrapping (`fmt.Errorf("...: %w", err)`) and returning to the caller; avoid `log.Fatal` in libraries.
- Tests: `HadesAPI/router_test.go` spins up NATS via `testcontainers-go`; expect Docker to be running locally.

## Things to avoid

- Don't bypass the operator path by writing a new direct-K8s scheduler; the operator is the strategic direction.
- Don't add fields to `BuildJobSpec` without regenerating the CRD (CI will block the PR).
- Don't introduce package-level mutable globals for dependencies - the codebase has been moving away from them (see `setupRouter` taking `JobPublisher` as a parameter).
- `HadesLogManager` is deployed (Helm + image) but ClusterIP-internal and single-replica (in-memory aggregation); don't put it on the ingress or scale it past one replica. The status-webhook dispatcher living there keeps no durable state in memory (its retry schedule is in JetStream), but it still assumes one replica. The same single-replica constraint applies to the API's in-memory dashboard read-model. (It just isn't in the `compose.yml` stack.)
- The dashboard treats all job/step `Metadata` and step `script` bodies as potentially secret: redact via `shared/redact` before returning any payload; never add an endpoint that returns raw metadata or an unscrubbed script. Job logs are proxied verbatim (a known, documented gap).


## Pull Requests

- When opening a PR make sure to use the pull_request_template.md in the .github folder. 