# HadesScheduler

HadesScheduler consumes queued jobs from NATS JetStream and executes each job's steps in containers. It is the component that actually runs work; the [API](../HadesAPI/Readme.md) only enqueues it.

## How it works

The scheduler subscribes to the priority-bucketed job subjects (`hades.jobs.{high,medium,low}`), pulls jobs up to `CONCURRENCY` at a time, and dispatches each to an **executor** chosen by the `HADES_EXECUTOR` environment variable.

```text
NATS (hades.jobs.*) ──► HadesScheduler ──► executor ──► containers (one per step)
```

### Job leases and redelivery

A pulled job stays leased until it is acknowledged. Because the Docker executor blocks for the whole job, the worker signals `InProgress` to JetStream every few seconds while the job runs, which keeps resetting the `NATS_ACK_WAIT` timer. A job is therefore only redelivered when the worker itself stops responding (crash, OOM kill, pod eviction, network partition) - never because it runs for a long time, including a job running all the way to its `timeout_seconds`.

`NATS_MAX_DELIVER` (default `3`) bounds how often a job that keeps taking down its worker is retried. The last delivery is not executed: it publishes a terminal `Failed` status (with a reason explaining the redeliveries) and drops the job, so a poisonous job neither loops forever nor disappears without a status.

## Executors

### Docker (`HADES_EXECUTOR=docker`) - local development

Runs each step as a Docker container on the local daemon. Steps of a job share a per-job named volume `shared-<uuid>`, so a file written by one step is visible to later steps. The volume is created before the first step and removed after the last one. A step marked `continue_on_error: true` does not fail the job if it exits non-zero. Package: [`docker/`](docker).

### Kubernetes (`HADES_EXECUTOR=k8s`) - production

The scheduler creates a `BuildJob` custom resource; the [HadesOperator](HadesOperator/Readme.md) reconciles it into a Kubernetes `Job`. Cluster access uses the in-cluster config, falling back to `KUBECONFIG` when run out-of-cluster. Package: [`k8s/`](k8s).

## Configuration

Common variables: `HADES_EXECUTOR` (default `docker`), `CONCURRENCY` (default `1`), `NATS_URL`, `NATS_ACK_WAIT` (default `1m`), `NATS_MAX_DELIVER` (default `3`), and the executor-specific `DOCKER_*` or `K8S_*` variables. See [docs/configuration.md](../docs/configuration.md) for the complete list.

## Run locally

```bash
make run-scheduler   # from the repository root (auto-starts NATS in Docker)
```
