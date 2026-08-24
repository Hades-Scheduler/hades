---
sidebar_position: 1
---

# Submitting Jobs

The Hades API exposes a small REST interface for submitting jobs. Every job is a JSON payload that defines a **name** and an ordered list of **steps**. For the full machine-readable contract, see the [API reference](../reference/api).

## API Base URL

When running locally with Docker Compose, the API is available at:

```
http://localhost:8081
```

(The API listens on `8080` inside the container and is published on `8081` by `compose.yml`. With `make run` it is on `8080`.) In production this is the domain you configured (e.g. `https://hades.example.com`).

## Authentication

If the API is started with a non-empty `AUTH_KEY`, `POST /build` requires HTTP Basic Auth with username `hades` and the key as the password. If `AUTH_KEY` is empty, the endpoint is open.

## Job Payload Structure

```json
{
  "name": "string",
  "priority": 3,
  "metadata": { "KEY": "value" },
  "steps": [
    {
      "id": 1,
      "name": "string",
      "image": "docker-image:tag",
      "script": "shell command or script"
    }
  ]
}
```

| Field | Required | Description |
|---|---|---|
| `name` | ✅ | Human-readable job name |
| `priority` | ❌ | `1` = low, `2` = medium, `3+` = high (default `3`); selects the NATS queue |
| `metadata` | ❌ | Key-value pairs injected as environment variables into every step |
| `steps[].id` | ✅ | Numeric step order (starts at 1) |
| `steps[].name` | ✅ | Human-readable step name |
| `steps[].image` | ✅ | Container image to run this step in |
| `steps[].script` | ❌ | Shell script to execute inside the container |
| `steps[].continue_on_error` | ❌ | Continue with the next step if this one fails |
| `steps[].cpu_limit` | ❌ | CPU limit in millicores (e.g. `1000` = 1 core) |
| `steps[].memory_limit` | ❌ | Memory limit, e.g. `512M`, `2G` |
| `callback_url` | ❌ | Absolute `http`/`https` URL the Log Manager POSTs the job's aggregated **logs** to when it finishes |
| `status_callback_url` | ❌ | Absolute `http`/`https` URL that receives the [job-status webhook](#job-status-webhook) when the job reaches a terminal status |

## Hello World Example

```bash
curl -X POST http://localhost:8081/build \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Hello World",
    "steps": [
      { "id": 1, "name": "Say Hello", "image": "alpine:latest", "script": "echo Hello from Hades!" }
    ]
  }'
```

A successful response returns the assigned job ID:

```json
{ "message": "Successfully enqueued job", "job_id": "7f3a1c2b-..." }
```

## Multi-Step Job

Steps run sequentially. Each step runs in its own container but shares a common `/shared` volume - use it to pass files between steps.

```bash
curl -X POST http://localhost:8081/build \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Multi-Step Example",
    "steps": [
      { "id": 1, "name": "Setup",    "image": "alpine:latest",     "script": "echo Setting up... > /shared/output.txt" },
      { "id": 2, "name": "Process",  "image": "ubuntu:latest",     "script": "cat /shared/output.txt && echo Processing... >> /shared/output.txt" },
      { "id": 3, "name": "Finalize", "image": "python:3.9-alpine", "script": "cat /shared/output.txt && echo Done!" }
    ]
  }'
```

## Job-Status Webhook

Polling `GET /jobs/{id}/status` is fine for a human. For a service that needs to
react the moment a job ends - the Artemis integration, a code-review bot - set
`status_callback_url` and Hades pushes the outcome to you.

```bash
curl -X POST http://localhost:8081/build \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Graded Exercise",
    "status_callback_url": "https://my-service.example.com/hades/job-status",
    "steps": [
      { "id": 1, "name": "Test", "image": "alpine:latest", "script": "exit 1" }
    ]
  }'
```

As soon as the job reaches `Succeeded`, `Failed`, or `Stopped`, Hades POSTs:

```json
{
  "event": "job.completed",
  "job_id": "7f3a1c2b-1d40-4a0d-8b7a-2b3c4d5e6f70",
  "name": "Graded Exercise",
  "status": "Failed",
  "reason": "step 1 exited with code 1",
  "queued_at": "2026-08-21T12:00:00Z",
  "started_at": "2026-08-21T12:00:05Z",
  "finished_at": "2026-08-21T12:00:41Z",
  "duration_ms": 36000,
  "attempt": 1
}
```

Delivery is **at-least-once**: any non-2xx response or timeout is retried with
exponential backoff and the `attempt` counter increases, so your handler must be
idempotent and **deduplicate on `job_id`**. After the configured attempt budget is
exhausted the event is dropped. Answer with any 2xx as soon as you have durably
recorded the event; do the slow work afterwards.

Redirects are **not** followed - a 3xx counts as a failed attempt - so
`status_callback_url` must be the final destination.

:::note
`status_callback_url` is **not** the same as `callback_url`. `callback_url`
forwards the job's aggregated **log lines** as a bare JSON array - it carries no
status and is only sent after the log stream has drained. `status_callback_url`
reports the **outcome** and fires on the terminal status itself. Set either, both,
or neither. Existing `callback_url` receivers are unaffected.
:::

For the full field reference, delivery guarantees, and the
`STATUS_WEBHOOK_*` configuration, see the
[Log Manager Readme](https://github.com/Hades-Scheduler/hades/blob/main/HadesLogManager/Readme.md#job-status-webhook).

## Monitoring Jobs (Log Manager)

Status and logs are served by the **Log Manager**, a separate service (default port `8081` when run via `make run`), **not** by the API. Its endpoints:

| Method & path | Description |
|---|---|
| `GET /jobs` | List all known job IDs |
| `GET /jobs/{id}/status` | Current build status (`Queued`, `Running`, `Succeeded`, `Failed`, `Stopped`) |
| `GET /jobs/{id}/logs` | Aggregated log entries for the job |
| `GET /health` | Liveness probe |

```bash
curl http://localhost:8081/jobs/<id>/status
curl http://localhost:8081/jobs/<id>/logs
```

:::note
The Log Manager is deployed by the Helm chart (`hades-log-manager`) and can also be run locally with `make run`. See the [API Reference](../reference/api) for the interactive Log Manager OpenAPI spec.
:::
