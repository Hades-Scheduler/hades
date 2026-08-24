# HadesLogManager

HadesLogManager collects build-job logs from NATS, aggregates them in memory per
job, and forwards them to each job's own callback URL when the job finishes. It
also serves a small HTTP API for inspecting logs and status. It additionally
delivers the [job-status webhook](#job-status-webhook), a separate outbound push
that reports a job's outcome.

Where the logs go is configured **per job**: a build request may set a
`callback_url` field (see the main Readme's payload examples). The LogManager
resolves that URL by looking the job up in the `HADES_JOBS` JetStream KV store
(the same bucket HadesAPI writes and the scheduler consumes) at forward time. A
job with no `callback_url` is simply not forwarded.

## Where it sits in the build-log flow

Build logs travel from a running job to the URL the job asked for (typically the
Artemis adapter). The LogManager is the hop that turns the per-job NATS log
stream into a single HTTP payload for that endpoint. Test results travel a
**separate** path and are re-joined inside the adapter.

```text
 build job (pod)                         NATS JetStream                     HTTP
 ┌───────────────┐   operator reads    ┌────────────────────┐          ┌──────────────┐         ┌──────────────┐
 │ step-1 clone  │   pod logs via      │ stream:            │  watch   │ HadesLog     │  POST   │ job          │
 │ step-2 execute│──► K8s API, parses ─┤ HADES_JOB_LOGS     ├─────────►│ Manager      │ /logs   │ callback_url │──► Artemis
 │ step-3 result │   & publishes       │ subj: hades.logs.<jobID>      │ (aggregator) │         │ (adapter)    │
 └──────┬────────┘                     └────────────────────┘          └──────────────┘         └──────▲───────┘
        │ step-3 (junit-result-parser) ───────────────────────── POST /adapter/test-results ──────────┘
        │                                                                 (test results)
```

### Step by step

0. **Job starts.** The scheduler (operator mode) creates a `BuildJob` CR; the
   HadesOperator reconciles it into a Kubernetes `Job` with one container per step
   (`step-1` clone, `step-2` execute, `step-3` result) and publishes a `running`
   status to NATS.
1. **Operator streams container logs live.** As each container runs, the operator
   **follows** its stdout/stderr from the Kubernetes API (`Follow: true`) and parses
   lines into `buildlogs.Log{JobID, ContainerID, Logs[]}` incrementally, rather than
   reading the whole log once after the container stops. (The Docker executor does the
   same via `ContainerLogs` with `Follow: true`.)
2. **Operator publishes logs to NATS JetStream** on subject `hades.logs.<jobID>`
   (stream `HADES_JOB_LOGS`), emitting many small per-container batches over the
   container's lifetime (flushed on ~50 lines or ~1s). A zero-entry `Log` is published
   when a container starts so a step that produces no output keeps its slot. Status
   changes go to `hades.jobstatus.<status>`.
3. **LogManager watches, keyed off status.** On `running` it starts a durable
   JetStream consumer on `hades.logs.<jobID>` and streams batches into the in-memory
   aggregator, **coalesced per container** (all batches for one `ContainerID` merge
   into a single `Log`, preserving first-seen container order). On `succeeded`/`failed`
   it stops watching, marks the job complete, and forwards the logs.
4. **LogManager forwards to the job's callback URL.** It resolves the job's
   `callback_url` from the `HADES_JOBS` KV store and POSTs the aggregated
   `[]buildlogs.Log` there (typically the adapter's `POST /adapter/logs`). Jobs
   without a `callback_url` are not forwarded.
5. **Adapter stores execution logs.** It takes the execute step (`logs[1]`) as the
   execution logs. Per-container grouping matters here: if all steps were flattened
   into one `Log`, `logs[1]` would not exist and the adapter would report
   "Execution logs missing".
6. **Test results arrive separately.** The job's `junit-result-parser` step POSTs
   a `ResultDTO` to `POST /adapter/test-results`.
7. **Adapter joins and sends to Artemis.** Only once both logs and results exist for
   a job does the adapter merge them and POST the result to Artemis.

### Key points

- Logs are **pulled** by the operator (scraped from the K8s log API), not pushed by
  the build container.
- Logs stream **live**: they are published incrementally as a container produces them,
  not buffered until the step completes. The HadesAPI dashboard tails them over SSE
  (`GET /api/jobs/:id/logs/stream`), backed by its own ephemeral JetStream consumer on
  `hades.logs.<jobID>` (independent of this service).
- The LogManager is triggered by **job status events**, not a direct call.
- The forwarding destination is **per job** (`callback_url`), resolved from the
  `HADES_JOBS` KV store at forward time. A job without a `callback_url` runs and
  logs fine but its logs are never forwarded.
- Artemis only receives a result when **both** the build logs and the test results
  land, so a job missing the `result` step will run and log fine but never post to
  Artemis.

## Job-status webhook

The job-status webhook answers a question the log callback cannot: **did this job
finish, and how did it end?** It is a second, independent outbound push,
configured per job with `status_callback_url` on the build request.

### Why `callback_url` cannot serve this purpose

| | `callback_url` (log forwarding) | `status_callback_url` (job-status webhook) |
| --- | --- | --- |
| **When** | After `stopWatchingJobLogs` closes the watcher's `drain` channel and blocks on `watcher.wg.Wait()`, i.e. completion **plus** the JetStream log drain (bounded by the 30 s `defaultDrainTimeout` and growing with log volume). | On the terminal `hades.jobstatus.{Succeeded,Failed,Stopped}` event itself. Nothing waits for logs. |
| **Body** | `json.Marshal(GetJobLogs(jobID))` - a bare array of log lines. No status, no exit code, no timestamps, no job name; success and failure are indistinguishable. | A JSON object carrying the terminal status, an optional failure reason, lifecycle timestamps, and the job name. |
| **Reliability** | Fire and forget. A non-2xx is logged and dropped; no retry, no dead-letter. | At-least-once with exponential backoff and a bounded attempt budget, backed by a durable JetStream consumer. |

Log forwarding is unchanged - it still drains first, still POSTs the same log
array to `callback_url`, and is not affected by the webhook in any way.

### Payload

`POST <status_callback_url>` with `Content-Type: application/json`:

```json
{
  "event": "job.completed",
  "job_id": "7f3a1c2b-1d40-4a0d-8b7a-2b3c4d5e6f70",
  "name": "Example Job",
  "status": "Failed",
  "reason": "ImagePullBackOff: no such image",
  "queued_at": "2026-08-21T12:00:00Z",
  "started_at": "2026-08-21T12:00:05Z",
  "finished_at": "2026-08-21T12:00:41Z",
  "duration_ms": 36000,
  "attempt": 1
}
```

| Field | Always present | Description |
| --- | --- | --- |
| `event` | ✅ | Event-type discriminator. Currently always `job.completed`. Switch on this first so future event types do not break your receiver. |
| `job_id` | ✅ | The Hades job UUID. **This is the deduplication key.** |
| `name` | ❌ | The submitted job name. Absent when the job payload has already left the `HADES_JOBS` KV bucket. |
| `status` | ✅ | The terminal status: `Succeeded`, `Failed`, or `Stopped`. |
| `reason` | ❌ | Why a job ended as it did, e.g. `ImagePullBackOff: ...` or `job timed out after 60 seconds`. It is the publisher's `X-Hades-Reason` status header, bounded at 500 runes (ellipsis included) before sending, and forwarded for every terminal status. Redaction is best-effort and publisher-dependent: the Docker scheduler scrubs secret-looking tokens, while the operator forwards a Kubernetes Job condition message as-is. Treat it as human-readable diagnostic text, not as a sanitized or machine-parseable field. Absent whenever no reason was published, which today is every `Succeeded` event - switch on `status`, not on `reason` being absent, to decide whether a job failed. |
| `queued_at` | ❌ | NATS server timestamp of the job's `Queued` event. |
| `started_at` | ❌ | NATS server timestamp of the job's `Running` event. |
| `finished_at` | ✅ | NATS server timestamp of the terminal status event. |
| `duration_ms` | ❌ | `finished_at - started_at` in milliseconds. Present only when `started_at` is known. |
| `attempt` | ✅ | 1-based delivery attempt. `1` is the first delivery; anything higher is a **redelivery** of an event you may already have processed. |

The same values also ride in request headers so a receiver can route or drop a
delivery without parsing the body: `X-Hades-Event`, `X-Hades-Job-Id`,
`X-Hades-Attempt`, and `X-Hades-Delivery` (`<job_id>/<attempt>`).

#### About the timestamps

All three timestamps are **NATS server timestamps of the corresponding status
event**, not wall-clock readings taken while sending. That makes `finished_at`
authoritative and immune to dispatcher lag or retries: it says when the job's
outcome was published, not when you were told.

`queued_at` and `started_at` are **best-effort** and omitted when this process did
not observe the corresponding event - most plausibly because the log manager
restarted while the job was running. They are deliberately *not* taken from the
submitted payload's `timestamp` field, which is client-supplied and never
validated. Treat `finished_at` as a fact and the other two as a convenience.

`priority` is not included: it is submitted on the REST payload but never stored,
so it cannot be recovered when the terminal status arrives.

### Delivery semantics

- **At-least-once.** Duplicates are possible and expected. **Deduplicate on
  `job_id`** and treat `attempt > 1` as a redelivery.
- **Retry with backoff.** A transport error, a timeout, or any non-2xx response is
  retried. The delay starts at `STATUS_WEBHOOK_INITIAL_BACKOFF` and doubles per
  attempt up to `STATUS_WEBHOOK_MAX_BACKOFF`. Any 2xx is success.
- **Bounded.** After `STATUS_WEBHOOK_MAX_ATTEMPTS` failed attempts the event is
  dropped with an `ERROR` log line. There is no dead-letter queue.
- **Durable.** Status events are captured in the `HADES_JOB_STATUS` JetStream
  stream and delivered through the durable `HADES_STATUS_WEBHOOK` consumer, so a
  pending retry survives a log-manager restart. The consumer is created with
  `DeliverNew`, so a first-time deployment does not replay a backlog of jobs that
  finished before it existed.
- **Isolated.** Deliveries run concurrently (`STATUS_WEBHOOK_CONCURRENCY`) and off
  the job-execution path entirely: the scheduler and operator publish a status
  over core NATS and move on. A dead or slow receiver delays only its own job's
  notification.
- **Not sent** when a job has no `status_callback_url`, when the job payload is no
  longer in the KV bucket, or when the stored URL is malformed (a bad URL is not
  retried, since retrying cannot fix it).
- **Redirects are not followed.** A 3xx is treated as a failed attempt and
  retried. Following one would turn the POST into a bodyless GET (Go's default
  policy for 301/302/303), so a redirecting receiver would answer 200 while never
  seeing the event, and it would send the payload to a host the operator never
  configured. Point `status_callback_url` at the final destination.

### Why this lives here

The dispatcher runs in HadesLogManager because it is the only component already
positioned for the job: it subscribes to the status lifecycle, it already resolves
per-job callback URLs from the `HADES_JOBS` KV bucket, and it sits off the
job-execution critical path. Putting it with the publishers instead would mean
duplicating it across the Docker scheduler and the operator and running an
outbound HTTP call inside job reconciliation.

The service's single-replica constraint is a help rather than a hindrance here:
one dispatcher means one delivery attempt at a time per event. Unlike the log
aggregation it sits next to, the webhook keeps **no** durable state in memory -
the retry schedule lives in JetStream. The only in-memory state is the
`queued_at`/`started_at` cache, which is bounded, swept, and optional by design
(the corresponding fields are simply omitted when it is cold).

### Publishers keep using core NATS

Adding the `HADES_JOB_STATUS` stream does not change how anything publishes: the
API, scheduler, and operator still `PublishMsg` on `hades.jobstatus.*` over core
NATS. A JetStream stream simply also stores every message on its subjects, so
existing core subscribers - the log manager's own log watching and the dashboard's
live feed - continue to receive every event unchanged.

## HTTP API

| Method | Path                     | Description                                  |
| ------ | ------------------------ | -------------------------------------------- |
| GET    | `/jobs`                  | List known job IDs (active and completed).   |
| GET    | `/jobs/:jobId/logs`      | Aggregated log entries for a job.            |
| GET    | `/jobs/:jobId/status`    | Current build status for a job, or 404.      |
| GET    | `/health`                | Liveness probe.                              |

## Configuration

| Env var                  | Default  | Description                                    |
| ------------------------ | -------- | ---------------------------------------------- |
| `NATS_URL`               | `nats://localhost:4222` | NATS server URL.                |
| `NATS_TLS_ENABLED`       | `false`  | Enable TLS for the NATS connection.            |
| `HADESLOGMANAGER_API_PORT` | `8081` | HTTP API port.                                 |
| `LOG_BATCH_SIZE`         | `100`    | Log entries buffered before a flush.           |
| `LOG_RETENTION`          | `1h`     | How long completed job logs are kept in memory.|
| `MAX_JOB_LOGS`           | `1000`   | Max log entries retained per container of a job (oldest drop first; container slots are never removed). |
| `DEBUG`                  | `false`  | Enable verbose (debug-level) logging.          |

The forwarding destination is no longer a global env var. Each job sets its own
`callback_url` in the build request; the LogManager reads it from the
`HADES_JOBS` JetStream KV store when forwarding. Jobs without a `callback_url`
are not forwarded.

### Job-status webhook

| Env var                            | Default | Description                                                                                       |
| ---------------------------------- | ------- | ------------------------------------------------------------------------------------------------- |
| `STATUS_WEBHOOK_ENABLED`           | `true`  | Deliver job-status webhooks. When `false`, no stream or consumer is created and `status_callback_url` is ignored. The feature is inert for jobs that do not set `status_callback_url`, so it is safe to leave on. |
| `STATUS_WEBHOOK_MAX_ATTEMPTS`      | `6`     | Total delivery attempts per job (first try plus retries) before the event is dropped.             |
| `STATUS_WEBHOOK_TIMEOUT`           | `10s`   | Bound on a single delivery, including the callback-URL lookup.                                    |
| `STATUS_WEBHOOK_INITIAL_BACKOFF`   | `5s`    | Delay before the second attempt; doubles per attempt.                                             |
| `STATUS_WEBHOOK_MAX_BACKOFF`       | `5m`    | Ceiling for the retry delay.                                                                      |
| `STATUS_WEBHOOK_CONCURRENCY`       | `16`    | Deliveries in flight at once. This is what keeps one dead receiver from delaying other jobs.      |
| `STATUS_WEBHOOK_MAX_PENDING`       | `1000`  | Status events that may be awaiting acknowledgement (in flight or waiting out a retry backoff).    |

Like the log callback, the destination is per job: set `status_callback_url` on
the build request. There is no global status-webhook URL.
