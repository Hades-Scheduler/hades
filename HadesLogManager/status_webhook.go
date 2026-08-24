package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/hades-scheduler/hades/shared/buildstatus"
)

// EventJobCompleted is the only event type the status webhook emits today. It
// marks a job reaching a terminal status; the Status field carries the outcome
// (Succeeded, Failed, or Stopped). Receivers should switch on Event first so
// additional event types can be added later without breaking them.
const EventJobCompleted = "job.completed"

// Webhook request headers. They duplicate fields of the JSON body so a receiver
// can route, deduplicate, or drop a delivery without parsing it.
const (
	headerEvent    = "X-Hades-Event"
	headerJobID    = "X-Hades-Job-Id"
	headerAttempt  = "X-Hades-Attempt"
	headerDelivery = "X-Hades-Delivery"
)

// JobStatusEvent is the JSON body POSTed to a job's status_callback_url when the
// job reaches a terminal status.
//
// Delivery is at-least-once: the same JobID may arrive more than once (a
// receiver that answered slowly, a 5xx, or a redelivery after a restart).
// Receivers must deduplicate on JobID and treat Attempt > 1 as a redelivery of
// an event they may already have processed.
type JobStatusEvent struct {
	// Event is the event-type discriminator, always EventJobCompleted today.
	Event string `json:"event"`
	// JobID is the Hades job UUID. It is the deduplication key.
	JobID string `json:"job_id"`
	// Name is the human-readable job name from the submitted payload. Empty when
	// the job payload is no longer in the HADES_JOBS KV bucket.
	Name string `json:"name,omitempty"`
	// Status is the terminal status: Succeeded, Failed, or Stopped.
	Status string `json:"status"`
	// Reason explains an outcome, in practice a non-success one (e.g. an
	// image-pull error or a timeout). It is the publisher-supplied
	// X-Hades-Reason status header, bounded by buildstatus.MaxReasonLen runes
	// before it is sent. Redaction is best-effort and publisher-dependent: the
	// Docker scheduler runs the reason through redact.Default(), while the
	// operator forwards a Kubernetes Job condition message as-is. Treat it as
	// human-readable diagnostic text, not as a sanitized or machine-parseable
	// field. It is whatever the publisher attached, forwarded for every terminal
	// status - empty whenever none was attached, which today is every Succeeded
	// event. Switch on Status rather than on Reason being empty to decide
	// whether a job failed.
	Reason string `json:"reason,omitempty"`
	// QueuedAt, StartedAt are the NATS server timestamps of this job's Queued and
	// Running status events. They are omitted when this process did not observe
	// the corresponding event (for example because it restarted mid-job), so
	// treat them as best-effort.
	QueuedAt  *time.Time `json:"queued_at,omitempty"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	// FinishedAt is the NATS server timestamp of the terminal status event. It is
	// always present and is the authoritative completion time: it is stamped when
	// the status was published, not when this webhook was sent, so it is
	// unaffected by retries or by dispatcher lag.
	FinishedAt time.Time `json:"finished_at"`
	// DurationMs is FinishedAt - StartedAt in milliseconds, present only when
	// StartedAt is known.
	DurationMs *int64 `json:"duration_ms,omitempty"`
	// Attempt is the 1-based delivery attempt for this event. 1 is the first
	// delivery; anything higher is a redelivery of an event the receiver may
	// already have seen.
	Attempt int `json:"attempt"`
}

// StatusWebhookConfig configures job-status webhook delivery. The webhook is
// inert unless a job sets status_callback_url, so it is enabled by default.
type StatusWebhookConfig struct {
	Enabled bool `env:"STATUS_WEBHOOK_ENABLED" envDefault:"true"`
	// MaxAttempts bounds total delivery attempts per job (first try + retries).
	MaxAttempts int `env:"STATUS_WEBHOOK_MAX_ATTEMPTS" envDefault:"6"`
	// Timeout bounds a single POST, including the callback-URL lookup.
	Timeout time.Duration `env:"STATUS_WEBHOOK_TIMEOUT" envDefault:"10s"`
	// InitialBackoff is the delay before the second attempt; it doubles per
	// attempt up to MaxBackoff.
	InitialBackoff time.Duration `env:"STATUS_WEBHOOK_INITIAL_BACKOFF" envDefault:"5s"`
	MaxBackoff     time.Duration `env:"STATUS_WEBHOOK_MAX_BACKOFF" envDefault:"5m"`
	// Concurrency bounds how many deliveries are in flight at once. It is what
	// keeps one dead receiver from delaying every other job's notification.
	Concurrency int `env:"STATUS_WEBHOOK_CONCURRENCY" envDefault:"16"`
	// MaxPending bounds how many status events may be awaiting acknowledgement
	// (in flight or waiting out a retry backoff) on the JetStream consumer.
	MaxPending int `env:"STATUS_WEBHOOK_MAX_PENDING" envDefault:"1000"`
}

// normalized repairs a misconfigured config so it cannot produce a hot retry
// loop or a consumer that never delivers. MaxAttempts, Timeout, InitialBackoff,
// and Concurrency fall back to their envDefault values when non-positive.
// MaxBackoff and MaxPending are instead raised to a relative floor -
// InitialBackoff and Concurrency respectively - so an operator who lowers one
// of a pair gets a coherent config rather than the shipped default. Enabled is
// left alone so an explicit "false" survives.
func (c StatusWebhookConfig) normalized() StatusWebhookConfig {
	if c.MaxAttempts < 1 {
		c.MaxAttempts = 6
	}
	if c.Timeout <= 0 {
		c.Timeout = 10 * time.Second
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = 5 * time.Second
	}
	if c.MaxBackoff < c.InitialBackoff {
		c.MaxBackoff = c.InitialBackoff
	}
	if c.Concurrency < 1 {
		c.Concurrency = 16
	}
	if c.MaxPending < c.Concurrency {
		c.MaxPending = c.Concurrency
	}
	return c
}

// backoffFor returns the delay before the attempt following the given 1-based
// attempt number: InitialBackoff doubled once per elapsed attempt, capped at
// MaxBackoff. It is deterministic (no jitter) because a single-replica
// dispatcher has no thundering-herd peer to spread out against.
func backoffFor(attempt int, initial, maximum time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := initial
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= maximum {
			return maximum
		}
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

// webhookSender delivers one status event to one URL.
type webhookSender interface {
	Send(ctx context.Context, url string, event JobStatusEvent) error
}

// maxDrainedResponseBytes bounds how much of a webhook response body is read
// before closing it. Draining is what lets the connection be reused; the body
// itself is never parsed.
const maxDrainedResponseBytes = 64 << 10

// httpWebhookSender POSTs the event as JSON over HTTP.
type httpWebhookSender struct {
	client *http.Client
}

func newHTTPWebhookSender(timeout time.Duration) *httpWebhookSender {
	return &httpWebhookSender{client: &http.Client{
		Timeout: timeout,
		// Do not follow redirects. Go's default policy turns a 301/302/303 into a
		// GET and drops the body, so a receiver that redirects would answer 200
		// while never seeing the event - a delivery reported as successful that
		// silently delivered nothing. Returning the 3xx instead surfaces it as a
		// non-2xx, which the dispatcher retries and eventually reports. It also
		// keeps the payload from being sent to a host the operator never
		// configured. status_callback_url must name the final destination.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

// Send POSTs event to url as JSON. A non-2xx response is an error so the caller
// retries; the response body is drained but never parsed.
func (s *httpWebhookSender) Send(ctx context.Context, url string, event JobStatusEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling status event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating status webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerEvent, event.Event)
	req.Header.Set(headerJobID, event.JobID)
	req.Header.Set(headerAttempt, strconv.Itoa(event.Attempt))
	req.Header.Set(headerDelivery, event.JobID+"/"+strconv.Itoa(event.Attempt))

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending status webhook: %w", err)
	}
	defer func() {
		// Drain a bounded prefix before closing so the connection can go back to
		// the keep-alive pool instead of being torn down on every delivery. The
		// limit keeps a chatty receiver from making this read unbounded.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainedResponseBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("status webhook endpoint returned %s", resp.Status)
	}
	return nil
}

// lifecycleTracker remembers when a job was queued and when it started, so the
// terminal event can carry those timestamps. It is fed from the same status
// stream that drives delivery, holds nothing durable, and is bounded in both
// size and age: a job whose terminal event never arrives is eventually evicted.
type lifecycleTracker struct {
	mu      sync.Mutex
	entries map[string]*lifecycleEntry
	maxSize int
	ttl     time.Duration
}

type lifecycleEntry struct {
	queuedAt  time.Time
	startedAt time.Time
	updatedAt time.Time
}

const (
	// lifecycleMaxEntries caps the tracker so a flood of jobs that never reach a
	// terminal status cannot grow it without bound.
	lifecycleMaxEntries = 10000
	// lifecycleTTL evicts jobs that never produced a terminal status event.
	lifecycleTTL = 24 * time.Hour
	// lifecycleSweepInterval is how often expired entries are swept.
	lifecycleSweepInterval = time.Minute
)

func newLifecycleTracker() *lifecycleTracker {
	return &lifecycleTracker{
		entries: make(map[string]*lifecycleEntry),
		maxSize: lifecycleMaxEntries,
		ttl:     lifecycleTTL,
	}
}

// observe records a non-terminal status transition for jobID at the given
// event timestamp. Only the first timestamp per transition is kept, so a
// redelivered Running event does not move StartedAt forward.
func (t *lifecycleTracker) observe(jobID string, status buildstatus.JobStatus, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry := t.entries[jobID]
	isNew := entry == nil
	if isNew {
		entry = &lifecycleEntry{}
		t.entries[jobID] = entry
	}
	switch status {
	case buildstatus.StatusQueued:
		if entry.queuedAt.IsZero() {
			entry.queuedAt = at
		}
	case buildstatus.StatusRunning:
		if entry.startedAt.IsZero() {
			entry.startedAt = at
		}
	}
	entry.updatedAt = at
	// Evict only after updatedAt is set, so the entry just inserted is not
	// itself mistaken for the least recently updated one.
	if isNew {
		t.evictOldestLocked()
	}
}

// take returns the recorded timestamps for jobID and forgets the job. Zero
// values mean the transition was never observed by this process.
func (t *lifecycleTracker) take(jobID string) (queuedAt, startedAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, ok := t.entries[jobID]
	if !ok {
		return time.Time{}, time.Time{}
	}
	delete(t.entries, jobID)
	return entry.queuedAt, entry.startedAt
}

// peek returns the recorded timestamps for jobID without forgetting it. It is
// used while a delivery may still be retried.
func (t *lifecycleTracker) peek(jobID string) (queuedAt, startedAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, ok := t.entries[jobID]
	if !ok {
		return time.Time{}, time.Time{}
	}
	return entry.queuedAt, entry.startedAt
}

// sweep drops entries older than the TTL.
func (t *lifecycleTracker) sweep(now time.Time) {
	cutoff := now.Add(-t.ttl)
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, entry := range t.entries {
		if entry.updatedAt.Before(cutoff) {
			delete(t.entries, id)
		}
	}
}

// sweepLoop sweeps expired entries until ctx is cancelled.
func (t *lifecycleTracker) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(lifecycleSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.sweep(time.Now())
		}
	}
}

// evictOldestLocked drops least-recently-updated entries while the map exceeds
// its cap. The caller must hold the lock.
func (t *lifecycleTracker) evictOldestLocked() {
	for len(t.entries) > t.maxSize {
		var oldestID string
		var oldest time.Time
		first := true
		for id, entry := range t.entries {
			if first || entry.updatedAt.Before(oldest) {
				oldestID, oldest, first = id, entry.updatedAt, false
			}
		}
		delete(t.entries, oldestID)
	}
}

// buildEvent assembles the webhook body for a terminal status.
func buildEvent(jobID, name string, status buildstatus.JobStatus, reason string, queuedAt, startedAt, finishedAt time.Time, attempt int) JobStatusEvent {
	event := JobStatusEvent{
		Event:      EventJobCompleted,
		JobID:      jobID,
		Name:       name,
		Status:     status.String(),
		Reason:     reason,
		FinishedAt: finishedAt,
		Attempt:    attempt,
	}
	if !queuedAt.IsZero() {
		t := queuedAt
		event.QueuedAt = &t
	}
	if !startedAt.IsZero() {
		t := startedAt
		event.StartedAt = &t
		ms := finishedAt.Sub(startedAt).Milliseconds()
		event.DurationMs = &ms
	}
	return event
}
