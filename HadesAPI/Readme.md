# HadesAPI

HadesAPI is the entry point of Hades: a small [Gin](https://gin-gonic.com/) HTTP server that accepts job submissions, validates them, and publishes them to NATS JetStream for the [scheduler](../HadesScheduler/Readme.md) to execute.

## Responsibilities

- Validate an incoming job payload (`name` is required; per-step `memory_limit` must be parseable).
- Assign the job a UUID.
- Publish it to the NATS queue matching its priority (`hades.jobs.{high,medium,low}`).

It does **not** run jobs itself and holds no state.

## Endpoints

| Method | Path | Auth | Description |
| ------ | ---- | ---- | ----------- |
| `GET` | `/ping` | none | Liveness probe (`{"status":"ok","timestamp":...}`). |
| `POST` | `/build` | Basic | Validate and enqueue a job. Returns the assigned `job_id`. |

The full request/response contract and the job payload schema are in [docs/api.md](../docs/api.md) and the [published API reference](https://hades-scheduler.github.io/hades/docs/reference/api).

### Example

```bash
# Add:  -u "hades:$AUTH_KEY"
curl -X POST -H "Content-Type: application/json" \
  -d '{"name":"Example","steps":[{"id":1,"name":"hi","image":"alpine:latest","script":"echo hello"}]}' \
  http://localhost:8080/build
```

## Authentication

`/build` requires HTTP Basic Auth with username `hades` and `AUTH_KEY` as the password. `AUTH_KEY` is required: with an empty key the API exits at start unless `ALLOW_UNAUTHENTICATED=true` (local development only; `make run` sets it). Bodies are capped at 1 MiB (413 above), duplicate step ids get 400, and the `DEBUG=true` Swagger UI sits behind the same Basic Auth.

## Priority

`POST /build` accepts an optional `priority` field: `1` = low, `2` = medium, `3+` = high (default `3`). The priority selects the NATS queue and is propagated to executors via job metadata (see `MetadataKeyPriority` in `../shared/prio.go`).

## Configuration

Common variables: `API_PORT` (default `8080`), `AUTH_KEY`, `ALLOW_UNAUTHENTICATED`, `NATS_URL`. See [docs/configuration.md](../docs/configuration.md) for the complete list.

## Run locally

```bash
make run-api      # from the repository root (auto-starts NATS in Docker)
```
