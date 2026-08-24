<p align="center">
  <img src="docs/assets/hades-icon.svg" alt="Hades logo" width="128" height="128" />
</p>

<h1 align="center">Hades: A Scalable Job Scheduler for Container Workloads</h1>

Welcome to Hades, a robust job scheduler designed with scalability in mind. Hades' primary mission is to provide a straightforward, scalable, and adaptable solution for executing containerized workloads in various environments, from educational programming courses to research computing clusters.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **📖 Documentation:** the full docs live at **[hades-scheduler.github.io/hades](https://hades-scheduler.github.io/hades/)** (a Docusaurus site, source in [`website/`](./website)). The in-repo [`docs/`](./docs/README.md) index and per-component READMEs cover the same material for offline/agent use.

## Design Goals

Hades embodies several core design principles:

- **Simplicity**: Hades focuses on delivering just the essentials required to execute containerized jobs efficiently, without unnecessary complexity.

- **Scalability**: Hades has scalability at its core, capable of queuing and executing a vast number of jobs in parallel, making it ideal for large-scale operations.

- **Container-Based**: Hades executes jobs within containers, ensuring a high level of isolation and security between workloads.

- **Kubernetes Native**: As a Kubernetes-native solution, Hades leverages the power and flexibility of Kubernetes as its primary execution platform for production workloads.

- **Extensibility**: Hades is designed to be highly extensible, allowing for easy integration with other execution platforms and workflow systems as needed.

## Architecture

Hades is built upon the following key components:

- **API**: Serving as the main entry point, the API handles all incoming job requests and provides status information.

- **Queue**: Using NATS as a message queue, this component is responsible for managing the queue of jobs, ensuring efficient scheduling and reliable delivery.

- **Scheduler**: The scheduler orchestrates the execution of jobs, coordinating with the executor components to run each job step in the appropriate environment.

  - **Docker Executor**: Designed for local development, the Docker executor is responsible for running jobs within Docker containers on a single host.

  - **Hades Operator**: The production-ready standard for Kubernetes. It implements a Kubernetes-native controller pattern using Custom Resource Definitions (CRDs). This mode offers superior scalability, automatic retries, and fine-grained RBAC integration.

- **Log Manager**: Subscribes to job status and log events on NATS, aggregates per-job logs in memory, and exposes them through an HTTP API (`GET /jobs`, `/jobs/:id/logs`, `/jobs/:id/status`, default port `8081`). It has its own Dockerfile and is deployed by the Helm chart (`hades-log-manager`). It is not part of the `compose.yml` stack, so run it locally with `make run`.

- **Dashboard**: An optional, secured web UI served by the API itself (embedded SPA + `/api/*` JSON/SSE endpoints). It shows queued/running/recently-completed jobs, a redacted job detail view, live logs, and system metrics, and updates live over Server-Sent Events. See [Dashboard](#dashboard) below.

## How It Works

Hades processes jobs through a sequence of well-defined steps:

1. **Job Submission**: Jobs are submitted to the API, defining a series of steps to execute.
2. **Queuing**: The job is queued in NATS for asynchronous processing.
3. **Scheduling**: The scheduler picks up the job and schedules it on the appropriate executor.
4. **Execution**: Each step of the job runs in its own container, with steps sharing data through a common volume.
5. **Completion**: Upon completion, results are stored and made available through the API.

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) for local development
- [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) and a Kubernetes cluster for production deployment
- [Minikube](https://minikube.sigs.k8s.io/docs/start/) for local Kubernetes testing (optional)

### Running in Docker Mode

To run Hades in Docker mode for local development:

1. Clone the repository:

   ```fish
   git clone https://github.com/Hades-Scheduler/hades.git
   cd hades
   ```

2. Copy the `.env.example` file to `.env` (the default configuration uses Docker as the executor, so no changes are necessary for local testing):

   ```fish
   cp .env.example .env
   ```

3. Start the Hades services:

   - **All components in the CLI** (NATS still runs in Docker):

     ```fish
     make run
     ```

     This launches `HadesAPI`, `HadesScheduler`, and `HadesLogManager` via `go run` and streams their logs to the terminal. Press `Ctrl-C` to stop them; run `make docker-stop` to also shut NATS down.

   - **Full stack in Docker**:

     ```fish
     make docker-run
     ```

     Use `make docker-logs` to follow the output and `make docker-stop` to tear the stack down.

### Running in Kubernetes Mode

For production deployments, Hades is designed to run natively within a Kubernetes cluster using **Helm**. This is the recommended way to achieve full scalability and reliability.

1. **Prerequisites**:
   - A Kubernetes cluster (v1.25+)
   - [Helm](https://helm.sh/docs/intro/install/) (v3.12+) installed locally.

2. **Deployment**: We provide a comprehensive Helm Chart that packages the API, Scheduler, Operator, and NATS broker. By default the scheduler runs in `operator` mode, delegating job lifecycle management to the HadesOperator via `BuildJob` custom resources.
   
   Replace `<version>` in the commands below with the chart version you want to
   install (for example `1.0.0`).

   ```fish
   # Install the published chart from GHCR (recommended)
   helm upgrade --install hades oci://ghcr.io/hades-scheduler/charts/hades \
     --version <version> -n hades --create-namespace
   ```

   Or install from a local checkout of this repository:

   ```fish
   helm repo add nats https://nats-io.github.io/k8s/helm/charts
   helm dependency build ./helm/hades/
   helm upgrade --install hades ./helm/hades -n hades --create-namespace
   ```

   > **Note:** Helm does not upgrade CRDs after the first install. When a release
   > changes the `BuildJob` CRD, apply it manually from the same chart version you
   > installed (not from the mutable `main` branch):
   > `helm show crds oci://ghcr.io/hades-scheduler/charts/hades --version <version> | kubectl apply -f -`

3. **Detailed Documentation**: For advanced configuration (Ingress, TLS, resource limits) and step-by-step setup, please refer to the: [Hades Helm Chart Guide](./helm/hades/Readme.md)

## Usage Examples

### Creating a Simple Job

Here's an example of submitting a basic job to Hades:

```json
{
  "name": "Example Job",
  "metadata": {
    "GLOBAL": "test"
  },
  "steps": [
    {
      "id": 1,
      "name": "Hello World",
      "image": "alpine:latest",
      "script": "echo 'Hello, Hades!'"
    }
  ]
}
```

Submit this job using:

```fish
curl -X POST -H "Content-Type: application/json" -d @job.json http://localhost:8080/build
```

#### Forwarding logs (`callback_url`)

To have the Log Manager forward a job's aggregated logs somewhere (for example the
Artemis adapter), add an optional top-level `callback_url` to the request. It must
be an absolute `http`/`https` URL that includes a host. If omitted, the job's logs
are not forwarded.

```json
{
  "name": "Example Job",
  "callback_url": "http://localhost:8082/adapter/logs",
  "steps": [
    {
      "id": 1,
      "name": "Hello World",
      "image": "alpine:latest",
      "script": "echo 'Hello, Hades!'"
    }
  ]
}
```

#### Job-status webhook (`status_callback_url`)

`callback_url` forwards **logs**. It is not a completion signal: its body is a bare
JSON array of log lines with no status, no exit code, and no timestamps, so a
receiver cannot tell a passing job from a failing one, and it is only sent after
the Log Manager has drained the job's JetStream log consumer (bounded by a 30 s
drain timeout that grows with log volume).

To be told **that a job finished and how it ended**, set the separate optional
top-level `status_callback_url`. Both fields are independent: a job may set
either, both, or neither, and existing `callback_url` behaviour is unchanged.

```json
{
  "name": "Example Job",
  "callback_url": "http://localhost:8082/adapter/logs",
  "status_callback_url": "http://localhost:8082/adapter/job-status",
  "steps": [
    {
      "id": 1,
      "name": "Hello World",
      "image": "alpine:latest",
      "script": "echo 'Hello, Hades!'"
    }
  ]
}
```

Hades POSTs a single JSON object as soon as the job reaches a terminal status
(`Succeeded`, `Failed`, or `Stopped`), without waiting for the log drain:

```json
{
  "event": "job.completed",
  "job_id": "7f3a1c2b-...",
  "name": "Example Job",
  "status": "Failed",
  "reason": "ImagePullBackOff: ...",
  "queued_at": "2026-08-21T12:00:00Z",
  "started_at": "2026-08-21T12:00:05Z",
  "finished_at": "2026-08-21T12:00:41Z",
  "duration_ms": 36000,
  "attempt": 1
}
```

Delivery is **at-least-once**: a failed delivery is retried with exponential
backoff and `attempt` counts up, so receivers must deduplicate on `job_id`. See
[`HadesLogManager/Readme.md`](HadesLogManager/Readme.md#job-status-webhook) for
the full schema, delivery guarantees, and configuration.

### Multi-Step Job Example

For more complex workflows, you can define multi-step jobs where each step runs in a different container:

```json
{
  "name": "Multi-Step Example",
  "steps": [
    {
      "id": 1,
      "name": "Step 1",
      "image": "alpine:latest",
      "script": "echo 'Setting up environment...' > /shared/output.txt"
    },
    {
      "id": 2,
      "name": "Step 2",
      "image": "ubuntu:latest",
      "script": "cat /shared/output.txt && echo 'Processing data...' >> /shared/output.txt"
    },
    {
      "id": 3,
      "name": "Step 3",
      "image": "python:3.9-alpine",
      "script": "cat /shared/output.txt && echo 'Finalizing...' >> /shared/output.txt && cat /shared/output.txt"
    }
  ]
}
```

## Dashboard

Hades ships an optional, secured **web dashboard** served by `HadesAPI` itself.
The API embeds a React/TypeScript single-page app (Vite, Tailwind, shadcn/ui) and
exposes a small JSON + Server-Sent-Events API under `/api`; no separate service is
introduced. The dashboard shows queued/running/recently-completed jobs, a job
detail view (steps, scripts, resource limits, and metadata with secrets redacted),
live logs, and system metrics, all updating live.

### How it gets its data

- **Job list, status, metrics, live updates** come from the API's subscription to
  the NATS `hades.jobstatus.*` lifecycle events. The API now also publishes
  `Queued` on enqueue so newly submitted jobs appear immediately.
- **Job detail** is read on demand from the `HADES_JOBS` JetStream KV bucket (the
  full submitted payload) and **redacted** before it leaves the process.
- **Logs** stream **live** over SSE (`GET /api/jobs/:id/logs/stream`): for a running
  job the API opens its own ephemeral JetStream consumer on `hades.logs.<jobID>`
  (full backlog + live tail) and pushes each new batch to the browser as it is
  produced - no polling. Completed jobs fall back to a one-shot snapshot proxied to
  the internal `HadesLogManager` (authenticated, so that service is never exposed
  directly). `HadesLogManager` remains the aggregator for the Artemis-forwarding path.

Read-side state is in-memory and recent-only (bounded by `DASHBOARD_JOB_RETENTION`),
so both `HadesAPI` and `HadesLogManager` must stay at a single replica.

### Enabling it

The dashboard is **disabled unless configured** (its `/api` routes return `503` and
the SPA is not served). Set three variables to enable it:

| Variable | Description |
|----------|-------------|
| `DASHBOARD_USERNAME` | Login username |
| `DASHBOARD_PASSWORD_HASH` | bcrypt hash of the password (e.g. `htpasswd -bnBC 12 "" 'yourpass' \| tr -d ':\n'`) |
| `DASHBOARD_SESSION_SECRET` | Random string (>=32 chars) used to sign session cookies |

Optional: `DASHBOARD_SESSION_TTL` (default `12h`), `DASHBOARD_JOB_RETENTION`
(default `1h`), `LOG_MANAGER_URL` (default the in-cluster log manager),
`SECRET_REDACT_MODE` (`smart` default, or `all`), `SECRET_KEY_PATTERNS`,
`DASHBOARD_TRUSTED_PROXIES`, and `DASHBOARD_COOKIE_INSECURE`.

Login uses a signed, `HttpOnly; Secure; SameSite=Strict` session cookie; all
`/api/*` routes require a valid session, with rate-limited login lockout.

### Security notes

- **Deploy behind TLS.** The session cookie is `Secure`, so login only works over
  HTTPS (or `http://localhost`). For a rare plain-HTTP dev setup, set
  `DASHBOARD_COOKIE_INSECURE=true` - never in production.
- **Set `DASHBOARD_TRUSTED_PROXIES`** to your ingress' address range when behind a
  reverse proxy. Otherwise `X-Forwarded-For` is ignored (the login lockout keys on
  the direct, un-spoofable address).
- Responses carry `Content-Security-Policy`, `X-Frame-Options: DENY`,
  `X-Content-Type-Options: nosniff`, and HSTS.

### Secret handling

Job metadata (job- and step-level) is injected into containers as environment
variables and routinely carries credentials. The dashboard redacts it
**server-side** before any JSON is sent: values are masked when the key looks
sensitive (`token`, `password`, `secret`, ...) **or** the value itself looks like a
secret (credentials embedded in a URL, a PEM block, a JWT, a high-entropy token).
Step **scripts** are scanned with the same heuristics and have inline secrets
masked. Keys stay visible so operators can see which variables exist;
`SECRET_REDACT_MODE=all` masks every metadata value.

**Residual exposure (by design):** job **logs** are shown *verbatim* - streamed live
over SSE for running jobs and proxied as a snapshot for completed ones - so a secret a
job echoes to stdout will be visible (the log panel warns about this).
Script redaction is best-effort heuristic scrubbing, not a guarantee. Sessions are
stateless HMAC tokens, so `logout` and expiry are enforced by the cookie/TTL but a
leaked token cannot be revoked before it expires - keep `DASHBOARD_SESSION_TTL`
modest and rotate `DASHBOARD_SESSION_SECRET` to invalidate all sessions at once.

### Local development

```fish
make ui-build   # build the SPA into HadesAPI/web/dist (embedded by the API)
make run        # run API + scheduler + log manager (+ NATS) with your dashboard env set
# then open http://localhost:8080/
```

For SPA development with hot reload, run `make ui-dev` (Vite dev server on `:5173`,
proxying `/api` to the API). See [`HadesAPI/web/README.md`](./HadesAPI/web/README.md).

In Kubernetes, set `hadesApi.dashboard.secretName` (a Secret with the three
`DASHBOARD_*` keys) in the Helm chart; the chart then wires the env and adds the
`/` and `/api` ingress paths.

## Configuration Options

Hades is configured through environment variables (or a `.env` file for local runs). The most common settings:

| Variable | Description | Default |
|----------|-------------|---------|
| `HADES_EXECUTOR` | Execution platform: `docker` or `k8s` | `docker` |
| `CONCURRENCY` | Number of jobs to process concurrently | `1` |
| `API_PORT` | Port for the Hades API | `8080` |
| `AUTH_KEY` | HTTP Basic Auth key for the API (empty = no auth) | `` |
| `NATS_URL` | NATS server URL | `nats://localhost:4222` |
| `DEBUG` | Verbose (debug-level) logging | `false` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint for tracing (empty = tracing off) | `` |

See **[docs/configuration.md](./docs/configuration.md)** for the complete, per-component reference (Docker/Kubernetes executor, operator, and Log Manager options). A ready-to-copy `.env.example` lives at the repository root.

### Measuring Hades overhead

Hades instruments how much overhead it adds around each job, per step and per phase, splitting the wall-clock into `overhead` (Hades/Kubernetes coordination) and `runtime` (the user's container executing). Every service exposes Prometheus histograms (`hades_phase_seconds`, `hades_job_overhead_seconds`, …) on its `/metrics` port and logs a per-job `job timing summary` with `overhead_pct`. Setting `OTEL_EXPORTER_OTLP_ENDPOINT` additionally emits an OpenTelemetry trace per job - a waterfall across API → scheduler → operator. `make run` and `make docker-run` start a Jaeger UI on <http://localhost:16686>. See **[Overhead timing & tracing](./docs/configuration.md#overhead-timing--tracing-all-components)** for the full phase taxonomy.

## Development Workflow

A top-level [`Makefile`](./Makefile) wraps the most common development tasks. Run `make help` to see every target.

| Target | Purpose |
| ------ | ------- |
| `make run` | Run `HadesAPI`, `HadesScheduler`, and `HadesLogManager` locally via `go run` (NATS auto-starts in Docker). |
| `make run-api` / `make run-scheduler` / `make run-logmanager` / `make run-operator` | Run a single component locally via `go run`. |
| `make docker-run` / `make docker-stop` / `make docker-logs` | Start, stop, or tail the full docker compose stack. |
| `make docker-run-api` / `make docker-run-scheduler` / `make docker-run-nats` | Start an individual service via docker compose. |
| `make build` | Compile every Go module in the workspace. |
| `make ui-build` / `make ui-dev` / `make ui-test` | Build, dev-serve, or test the dashboard SPA (`HadesAPI/web`). |
| `make docker-build` | Build all Hades container images. |
| `make test` | Run unit tests across every Go module. |
| `make test-race` | Same as `make test` with the race detector. |
| `make cover` | Generate and open the HadesAPI coverage report. |
| `make test-operator` / `make test-operator-e2e` | Run HadesOperator envtest unit tests, or Kind-based e2e tests. |
| `make fmt` / `make lint` | Format code with `gofmt` or run `go vet`. |
| `make docs-api` | Regenerate the OpenAPI specs for HadesAPI and HadesLogManager (run after changing a handler annotation or DTO). |
| `make docs-helm` | Regenerate the Helm chart values table from `values.yaml` comments (run after changing chart values). |
| `make vuln` | Run `govulncheck` (auto-installs it on first use). |
| `make deps-check` / `make deps-update` / `make deps-tidy` | List outdated direct dependencies, bump them, or run `go mod tidy` across all modules. |
| `make helm-deps` | Refresh the Helm chart subchart lock file. |
| `make ci` | Mirror the CI run locally (`lint` + `test`). |

Tests live alongside the code in each module, and CI (`.github/workflows/ci.yml`) runs the `shared`, `HadesAPI`, `HadesScheduler`, `HadesLogManager`, and `HadesOperator` suites (a build matrix) on every push and pull request.
The HadesOperator e2e target requires [Kind](https://kind.sigs.k8s.io/) to be installed locally.

## Deployment

### Deploy into a VM

For production deployments in a VM:
1. Ensure you have Docker installed in the VM
2. Copy the `.env.example` file to `.env` and update the configuration:

   ```fish
   cp .env.example .env
   ```
3. Change the `LETSENCRYPT_EMAIL` variable to your email address in your `.env` file.
4. Change the `HADES_API_HOST` variable to domain name or your IP address in your `.env` file.
5. Create Traefik configuration files

    ```fish
    touch traefik/acme.json
    chmod 600 traefik/acme.json
    ```
6. Deploy Hades:
   ```fish
   docker compose -f compose.yml -f docker-compose.deploy.yml up -d
   ```

### Ansible Deployment

Hades includes Ansible playbooks for automated deployment.
See the `ansible/hades/README.md` file for more details.

### Kubernetes (GitHub Actions)

Two manual workflows deploy the Helm chart to a Kubernetes cluster (one cluster,
two namespaces):

- **`Deploy to Kubernetes (prod)`** (`.github/workflows/deploy-k8s-prod.yml`) - deploys
  into namespace `hades` from the `k8s-prod` environment. The `version` input must be
  `latest` or a published GitHub Release tag; it is validated before anything touches the
  cluster. To deploy a release, run the workflow **from that release's tag** so the chart
  and CRDs match the released images.
- **`Deploy to Kubernetes (test)`** (`.github/workflows/deploy-k8s-test.yml`) - deploys
  into namespace `hades-test` from the `k8s-test` environment. The `version` input
  accepts any image tag (`latest`, a release tag, `pr-N`, or a branch/sha tag). The chart
  and CRDs come from the ref the workflow runs from.

Both call the reusable `deploy-k8s.yml`, which writes the kubeconfig, creates the app
Secrets, applies the CRDs (server-side, since Helm never upgrades CRDs), and runs
`helm upgrade --install --atomic`. Per-environment, non-secret config lives in committed
values files (`helm/hades/values-prod.yaml`, `helm/hades/values-test.yaml`).

Each GitHub environment needs these secrets (add them once; the app Secrets are created
on the first deploy and kept in sync afterwards):

| Secret | Purpose |
|---|---|
| `KUBE_CONFIG` | kubeconfig (plain YAML) for the target cluster |
| `AUTH_KEY` | protects the API `/build` endpoint (Secret `hades-auth`) |
| `DASHBOARD_USERNAME` | dashboard login user (Secret `hades-dashboard`) |
| `DASHBOARD_PASSWORD_HASH` | bcrypt hash of the dashboard password |
| `DASHBOARD_SESSION_SECRET` | dashboard session signing secret (>=32 chars) |

`k8s-prod` uses a required-reviewer protection rule, so a production deploy waits for
manual approval. TLS is handled in-cluster by cert-manager (the `letsencrypt-prod`
cluster-issuer), so no ACME secrets are needed in the workflow.

### Releasing the Helm Chart

The chart is published to GHCR as an OCI artifact by
`.github/workflows/release-chart.yml`. It runs on pushes to `main` that touch
`helm/**` (or via `workflow_dispatch`) and publishes to
`oci://ghcr.io/hades-scheduler/charts/hades`.

Checklist when cutting a chart release:

1. **Bump `version` in `helm/hades/Chart.yaml`** (SemVer). This is the single
   source of truth: the workflow **only publishes when the version does not
   already exist** in GHCR, so a change to `helm/**` without a version bump is a
   no-op. Never re-tag an already-published version - always bump.
2. **Update `appVersion`** if the deployed application changed (it tracks the
   app, not the chart, and does not need to follow SemVer).
3. **CRDs are not upgraded by Helm.** Files under `helm/hades/crds/` are only
   applied on first install and never on `helm upgrade`. If a release changes
   the `BuildJob` CRD, bump the chart version, note it in the release, and tell
   users to apply the CRD from the matching chart version on existing clusters:
   `helm show crds oci://ghcr.io/hades-scheduler/charts/hades --version <version> | kubectl apply -f -`.
4. **Subchart dependencies** (currently `nats`): if you change a dependency
   version in `Chart.yaml`, run `helm dependency update helm/hades` and commit
   the updated `Chart.lock`. CI vendors the subchart at package time; the
   `charts/` directory itself is not committed.
5. **Validate before merging**: `helm lint helm/hades`,
   `helm template helm/hades`, and ideally a throwaway
   `helm install` in a scratch namespace.
6. **Package visibility**: the GHCR chart package must be **public** for
   anonymous `helm install` (same as the container images). Set once in the org
   Packages settings after the first publish.
7. **Keep the install snippet in sync**: update the `--version` in the
   Kubernetes install instructions above when you cut a new version.

## Dependency Management

Hades uses [Renovate](https://docs.renovatebot.com/) (configured in `renovate.json`) to open automated PRs for dependency updates across Go modules, Helm charts, Docker base images, and GitHub Actions.
Prefer merging Renovate PRs whenever possible so lock files and changelog links stay consistent.

For manual checks (for example before cutting a release), the workspace is wired up through the top-level Makefile:

```fish
make deps-check     # list outdated direct dependencies in every Go module
make deps-update    # bump direct deps in every module and run go mod tidy
make helm-deps      # refresh helm/hades/Chart.lock
make vuln           # run govulncheck across every module
```

After running `make deps-update`, verify the workspace still builds and tests pass:

```fish
make build
make test
```

Major-version upgrades (for example `sigs.k8s.io/controller-runtime` v0.22 -> v0.24, or any `/v2`, `/v3` import path bump) often contain breaking API changes and should be reviewed one module at a time rather than via a blanket `make deps-update`.

Docker base images in the per-component `Dockerfile`s are tracked by Renovate; for a manual bump, look up the latest tag on the relevant registry and edit the `FROM` line.

## High-Level Architecture Diagram

```
┌─────────┐         ┌─────────┐          ┌───────────────┐
│         │ jobs    │         │  jobs    │               │
│  API    │────────▶│  NATS   │─────────▶│  Scheduler    │
│         │         │ Queue   │          │               │
└─────────┘         └────┬────┘          └───────┬───────┘
                         ▲                       │
                  status │ logs                  ▼
                         │            ┌──────────┴──────────┐
                  ┌──────┴──────┐     │                     │
                  │             │     ▼                     ▼
                  │    Log      │  ┌─────────────┐    ┌─────────────────┐
                  │   Manager   │  │   Docker    │    │   Kubernetes    │
                  │  (HTTP API) │  │  Executor   │    │  / Operator     │
                  │             │  └─────────────┘    └─────────────────┘
                  └─────────────┘
```


## Acknowledgments

- Special thanks to all contributors who have helped shape Hades
- Inspired by the need for a lightweight, scalable job execution system in educational environments
- Built with Go, Docker, Kubernetes, and NATS
