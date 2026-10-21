---
sidebar_position: 1
---

# Docker Mode (Local Development)

Docker mode is the fastest way to get Hades running on your local machine. It runs the services on a single host with the Docker executor.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) v20.10+
- [Docker Compose](https://docs.docker.com/compose/install/) v2.0+

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/Hades-Scheduler/hades.git
cd hades
```

### 2. Configure Environment Variables

Copy the example environment file and review the defaults:

```bash
cp .env.example .env
```

The default configuration uses `docker` as the executor, so no further changes are required for local testing. The most common variables:

| Variable | Default | Description |
|---|---|---|
| `HADES_EXECUTOR` | `docker` | Execution backend. Use `docker` for local mode. |
| `CONCURRENCY` | `1` | Number of jobs processed concurrently. The bundled `compose.yml` sets this to `4` unless overridden. |
| `API_PORT` | `8080` | Port the Hades API listens on. |
| `AUTH_KEY` | *(empty)* | HTTP Basic Auth key for `/build` (empty = no auth). |

See the [Configuration Reference](../reference/configuration) for the full list.

### 3. Start the Services

The top-level `Makefile` wraps the common workflows (run `make help` to list them). Two ways to start:

```bash
# Run API, Scheduler, and Log Manager locally via `go run` (NATS auto-starts in Docker)
make run

# Or run the full stack in Docker Compose
make docker-run     # equivalent to: docker compose -f compose.yml up -d
```

With `make docker-run` this starts:
- **hadesAPI** on port `8081` (mapped from internal `8080`)
- **hadesScheduler** connected to the Docker socket
- **nats** on ports `4222` (client) and `8222` (monitoring)

:::note
`make run` also starts the **Log Manager** (port `8081`) for inspecting job status and logs. The Log Manager is not part of the Docker Compose stack.
:::

### 4. Verify the Stack Is Running

```bash
docker compose ps
curl http://localhost:8081/ping    # API health check
```

You can also open the NATS monitoring dashboard at [http://localhost:8222](http://localhost:8222).

## Submit Your First Job

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

## Stopping the Stack

```bash
make docker-stop    # or: docker compose down
```

## Next Steps

- Learn how to submit more complex jobs in the [Usage Guide](../usage/submitting-jobs).
- Ready for production? Follow the [Helm Chart installation guide](./helm-chart).
