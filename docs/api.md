# API Reference

Hades exposes two small HTTP services. This page is the human-readable reference; a machine-readable **OpenAPI** spec is generated from the code for each service (see [Interactive spec](#interactive-openapi-spec)).

| Service | Default address | Purpose |
| ------- | --------------- | ------- |
| **HadesAPI** | `http://localhost:8080` | Submit jobs; health check. |
| **HadesLogManager** | `http://localhost:8081` | Inspect aggregated build logs and status. |

---

## HadesAPI

### `GET /ping`

Liveness probe. No auth.

```bash
curl http://localhost:8080/ping
```

```json
{ "status": "ok", "timestamp": "2026-08-07T10:00:00Z" }
```

### `POST /build`

Validate a job, assign it a UUID, and enqueue it on NATS by priority.

**Auth:** HTTP Basic Auth (`hades` / `AUTH_KEY`) when the server was started with a non-empty `AUTH_KEY`; otherwise open.

**Request body** - a `RESTPayload` (see [Job payload schema](#job-payload-schema)):

```json
{
  "priority": 3,
  "name": "Example Job",
  "metadata": { "GLOBAL": "test" },
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

```bash
curl -X POST -H "Content-Type: application/json" \
  -u hades:$AUTH_KEY \
  -d @job.json http://localhost:8080/build
```

**Responses**

| Status | Body | Meaning |
| ------ | ---- | ------- |
| `200 OK` | `{ "message": "Successfully enqueued job", "job_id": "<uuid>" }` | Job accepted and queued. |
| `400 Bad Request` | plain-text message naming the offending field | Invalid JSON or failed validation (e.g. missing `name`, unparseable `memory_limit`). |
| `500 Internal Server Error` | `Failed to enqueue job` | NATS publish failed. |

---

## HadesLogManager

All endpoints are read-only and unauthenticated.

| Method & path | Description | Success body |
| ------------- | ----------- | ------------ |
| `GET /jobs` | List all known job IDs (active and completed). | `{ "jobs": [ ... ] }` |
| `GET /jobs/{jobId}/logs` | Aggregated log entries for a job. | `{ "logs": [ ... ] }` |
| `GET /jobs/{jobId}/status` | Current build status for a job (`404` if unknown). | `{ "status": "Running" }` |
| `GET /health` | Liveness probe. | `{ "status": "ok" }` |

```bash
curl http://localhost:8081/jobs
curl http://localhost:8081/jobs/<uuid>/status
```

---

## Job payload schema

Submitted to `POST /build`. Field semantics come from `shared/payload/payload.go`.

### `RESTPayload`

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `priority` | int | no | `1` = low, `2` = medium, `3+` = high. Defaults to `3` when omitted. Selects the NATS queue. |
| *(embeds `QueuePayload` below)* | | | |

### `QueuePayload`

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `id` | UUID | no | Assigned by the server; ignore on input. |
| `name` | string | **yes** | Human-readable job name. |
| `timestamp` | RFC3339 | no | Job creation time. |
| `metadata` | map[string]string | no | Job-level metadata (also injected as environment variables). |
| `steps` | `[]Step` | no | Ordered steps to execute. |
| `callback_url` | string | no | Absolute http/https URL to forward aggregated logs/results to. |
| `status_callback_url` | string | no | Absolute http/https URL that receives the job-status webhook when the job reaches a terminal status. Independent of `callback_url`; see [`HadesLogManager/Readme.md`](../HadesLogManager/Readme.md#job-status-webhook). |
| `timeout_seconds` | int64 | no | Whole-job timeout in seconds. The job is killed and marked failed once exceeded. `0` (default) = no timeout. Enforced on both executors (Docker: context deadline; Kubernetes: Job `activeDeadlineSeconds`). |

### `Step`

| Field | Type | Description |
| ----- | ---- | ----------- |
| `id` | int | Execution order (starts at 1). |
| `name` | string | Human-readable step name. |
| `image` | string | Container image, e.g. `alpine:latest`. |
| `script` | string | Shell script to run in the container. |
| `continue_on_error` | bool | Continue with the next step if this one fails. |
| `metadata` | map[string]string | Step-specific environment variables. |
| `cpu_limit` | uint | CPU limit in whole cores (e.g. `1` = 1 core, `2` = 2 cores). |
| `memory_limit` | string | Memory limit, e.g. `512M`, `2G`. |
| `network` | string | Docker network mode: `none`, `bridge`, `host`, `default`, or a named network. Empty = executor default. **Docker executor only** - accepted but not enforced on Kubernetes. |
| `memory_swap` | string | Total memory+swap limit (same format as `memory_limit`); must be set together with `memory_limit` and be `>=` it. **Docker executor only.** |
| `pids_limit` | int64 | Max PIDs in the container; `0` = unlimited. **Docker executor only.** |

Steps of a job share a per-job volume, so a file written by one step is visible to later steps.

> **Executor parity:** `timeout_seconds`, environment variables (via `metadata`), `cpu_limit` and `memory_limit` are enforced on both the Docker and Kubernetes executors. `network`, `memory_swap` and `pids_limit` are enforced on the Docker executor only; Kubernetes accepts them (for schema parity) but does not apply them, because a pod's containers share one network namespace and Kubernetes has no per-container swap or PID-limit field.
>
> **CPU unit note:** `cpu_limit` is interpreted as **whole cores** by both executors. (An earlier revision documented millicores; the code has always treated the value as cores.)
>
> **Priority propagation:** once queued, the numeric priority and its name are attached to the job metadata under the keys `hades.tum.de/priority` and `hades.tum.de/priorityName` (constants `MetadataKeyPriority` / `MetadataKeyPriorityName` in `shared/prio.go`). The operator surfaces `hades.tum.de/priority` as a Job/Pod label.

---

## Interactive OpenAPI spec

HadesLogManager serves an interactive Swagger UI **only when `DEBUG=true`** (the spec is never exposed in production):

- HadesAPI: `http://localhost:8080/swagger/index.html`
- HadesLogManager: `http://localhost:8081/swagger/index.html`

The generated specs are committed under `HadesAPI/docs/` and `HadesLogManager/docs/` (`swagger.json` / `swagger.yaml`). Regenerate them after changing any handler annotation or DTO:

```bash
make docs-api
```
