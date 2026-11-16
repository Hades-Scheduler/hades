---
sidebar_position: 1
title: API Reference
---

# API Reference

Hades exposes two small HTTP services. Both are documented with **interactive OpenAPI** references, rendered directly from the specs generated from the code (`make docs-api` -> `make docs-site-sync`).

| Service | Interactive reference | Default address |
|---|---|---|
| **HadesAPI** - submit jobs | **[HadesAPI OpenAPI »](/api/hades)** | `http://localhost:8080` |
| **HadesLogManager** - inspect logs/status | **[Log Manager OpenAPI »](/api/log-manager)** | `http://localhost:8081` |

The raw specs are also available as JSON: [`/openapi/hades-api.json`](/openapi/hades-api.json) and [`/openapi/log-manager.json`](/openapi/log-manager.json).

## HadesAPI at a glance

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/ping` | none | Liveness probe. |
| `POST` | `/build` | Basic | Validate and enqueue a job. Returns `{ "message", "job_id" }`. |

See [Submitting Jobs](../usage/submitting-jobs) for the payload schema and examples, and the [interactive reference](/api/hades) for the full contract.

## HadesLogManager at a glance

| Method | Path | Description |
|---|---|---|
| `GET` | `/jobs` | List known job IDs. |
| `GET` | `/jobs/{jobId}/status` | Current build status, or `404`. |
| `GET` | `/jobs/{jobId}/logs` | Aggregated log entries. |
| `GET` | `/health` | Liveness probe. |

The Log Manager is deployed by the Helm chart (`hades-log-manager`) and can also be run locally with `make run`.
