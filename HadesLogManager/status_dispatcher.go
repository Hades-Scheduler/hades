package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/utils"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// StatusStreamName is the JetStream stream that durably captures the
	// "hades.jobstatus.*" lifecycle events. Publishers keep publishing over core
	// NATS exactly as before - a stream simply also stores every message on its
	// subjects - so core subscribers (the log manager's own log watching, the
	// dashboard live feed) are unaffected.
	StatusStreamName = "HADES_JOB_STATUS"
	// statusWebhookConsumer is the durable consumer backing webhook delivery.
	// Being durable is what makes redelivery survive a dispatcher restart.
	statusWebhookConsumer = "HADES_STATUS_WEBHOOK"
	// statusStreamMaxMsgs bounds the retained status events across all jobs.
	statusStreamMaxMsgs = 200000
	// statusStreamMaxAge bounds how long an undelivered status event stays
	// eligible for redelivery.
	statusStreamMaxAge = 24 * time.Hour
	// ackWaitFloor is the minimum time JetStream waits for an ack before
	// redelivering. The effective value also accounts for the HTTP timeout so a
	// slow-but-alive receiver cannot trigger a concurrent duplicate delivery.
	ackWaitFloor = time.Minute
)

// statusMsg is the subset of jetstream.Msg the dispatcher uses. Narrowing it
// keeps the delivery logic unit-testable without a NATS server.
type statusMsg interface {
	Subject() string
	Data() []byte
	Headers() nats.Header
	Metadata() (*jetstream.MsgMetadata, error)
	Ack() error
	NakWithDelay(delay time.Duration) error
	Term() error
}

var _ statusMsg = jetstream.Msg(nil)

// StatusWebhookDispatcher delivers a job-status webhook when a job reaches a
// terminal status.
//
// It is deliberately independent of log forwarding. Log forwarding waits for the
// JetStream log consumer to drain before POSTing to callback_url; this
// dispatcher reacts to the terminal status event itself, so a job's outcome is
// announced as soon as it is published rather than after the drain. Nothing here
// runs on the job-execution path: the scheduler and the operator publish a
// status over core NATS and move on.
type StatusWebhookDispatcher struct {
	cfg       StatusWebhookConfig
	js        jetstream.JetStream
	resolver  jobInfoResolver
	sender    webhookSender
	lifecycle *lifecycleTracker
}

// NewStatusWebhookDispatcher builds a dispatcher. resolver supplies each job's
// status_callback_url; sender may be nil, in which case an HTTP sender bounded
// by cfg.Timeout is used.
func NewStatusWebhookDispatcher(js jetstream.JetStream, resolver jobInfoResolver, sender webhookSender, cfg StatusWebhookConfig) *StatusWebhookDispatcher {
	cfg = cfg.normalized()
	if sender == nil {
		sender = newHTTPWebhookSender(cfg.Timeout)
	}
	return &StatusWebhookDispatcher{
		cfg:       cfg,
		js:        js,
		resolver:  resolver,
		sender:    sender,
		lifecycle: newLifecycleTracker(),
	}
}

// Run creates the status stream and its durable consumer and delivers webhooks
// until ctx is cancelled. It returns only once every in-flight delivery has
// finished, so the caller can include it in a graceful-shutdown WaitGroup.
func (d *StatusWebhookDispatcher) Run(ctx context.Context) error {
	if _, err := d.js.CreateOrUpdateStream(ctx, d.streamConfig()); err != nil {
		return fmt.Errorf("creating %s stream: %w", StatusStreamName, err)
	}

	consumer, err := d.js.CreateOrUpdateConsumer(ctx, StatusStreamName, d.consumerConfig())
	if err != nil {
		return fmt.Errorf("creating %s consumer: %w", statusWebhookConsumer, err)
	}

	// A buffered channel is the concurrency limiter. Delivery runs off the
	// consume loop so a receiver that never answers holds exactly one slot for at
	// most cfg.Timeout instead of stalling every other job's notification.
	// Acquiring a slot blocks rather than parking the message back on JetStream,
	// because a requeue would spend one of the message's MaxDeliver attempts on
	// congestion that has nothing to do with the receiver. The wait is bounded by
	// cfg.Timeout and therefore always well inside AckWait.
	slots := make(chan struct{}, d.cfg.Concurrency)

	// inFlight tracks delivery goroutines. jetstream.ConsumeContext.Stop() does
	// not guarantee the handler has returned, so a flag guarded by the same mutex
	// - not Stop() alone - is what makes "no Add after Wait" hold. A message
	// dropped after the flag is set stays unacked and is redelivered on the next
	// start.
	var (
		mu       sync.Mutex
		stopped  bool
		inFlight sync.WaitGroup
	)

	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}

		mu.Lock()
		if stopped {
			mu.Unlock()
			<-slots
			return
		}
		inFlight.Add(1)
		mu.Unlock()

		go func() {
			defer inFlight.Done()
			defer func() { <-slots }()
			defer func() {
				if r := recover(); r != nil {
					slog.Error("Panic while delivering status webhook", "panic", r)
				}
			}()
			d.handle(ctx, msg)
		}()
	}, jetstream.PullMaxMessages(d.cfg.Concurrency))
	if err != nil {
		return fmt.Errorf("consuming %s: %w", statusWebhookConsumer, err)
	}

	go d.lifecycle.sweepLoop(ctx)

	slog.Info("Status webhook dispatcher started",
		"stream", StatusStreamName,
		"consumer", statusWebhookConsumer,
		"max_attempts", d.cfg.MaxAttempts,
		"concurrency", d.cfg.Concurrency)

	<-ctx.Done()
	// Close the gate first, then stop the consumer, then wait for in-flight
	// sends. Ordering it this way leaves no window in which Consume can still
	// hand a message to a goroutine that starts after the gate was meant to be
	// shut - Stop() does not guarantee the handler will not be invoked again.
	// The gate, not Stop(), is what makes "no Add after Wait" hold, so either
	// order is safe; this one also makes the code say what it means. Anything
	// dropped or still unacked at exit is redelivered after the next start.
	mu.Lock()
	stopped = true
	mu.Unlock()
	cc.Stop()
	inFlight.Wait()
	slog.Info("Status webhook dispatcher stopped")
	return nil
}

// streamConfig describes the durable capture of the status subjects.
func (d *StatusWebhookDispatcher) streamConfig() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      StatusStreamName,
		Subjects:  []string{buildstatus.StatusSubject("*")},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxMsgs:   statusStreamMaxMsgs,
		MaxAge:    statusStreamMaxAge,
	}
}

// consumerConfig describes the durable webhook consumer. DeliverNew means a
// freshly created consumer does not replay a backlog of already-finished jobs.
func (d *StatusWebhookDispatcher) consumerConfig() jetstream.ConsumerConfig {
	ackWait := 2*d.cfg.Timeout + 30*time.Second
	if ackWait < ackWaitFloor {
		ackWait = ackWaitFloor
	}
	return jetstream.ConsumerConfig{
		Name:          statusWebhookConsumer,
		Durable:       statusWebhookConsumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckWait:       ackWait,
		MaxDeliver:    d.cfg.MaxAttempts,
		MaxAckPending: d.cfg.MaxPending,
	}
}

// handle processes one status event: it records lifecycle timestamps for
// non-terminal statuses and delivers the webhook for terminal ones.
func (d *StatusWebhookDispatcher) handle(ctx context.Context, msg statusMsg) {
	status := buildstatus.StatusFromSubject(msg.Subject())
	jobID := string(msg.Data())
	if jobID == "" || !status.IsValid() {
		slog.Warn("Discarding malformed status event", "subject", msg.Subject())
		d.term(msg, jobID)
		return
	}

	meta, err := msg.Metadata()
	if err != nil {
		slog.Warn("Discarding status event without JetStream metadata", "job_id", jobID, "error", err)
		d.term(msg, jobID)
		return
	}

	if !status.IsTerminal() {
		d.lifecycle.observe(jobID, status, meta.Timestamp)
		d.ack(msg, jobID)
		return
	}

	attempt := int(meta.NumDelivered)
	if d.deliver(ctx, msg, jobID, status, meta.Timestamp, attempt) {
		d.lifecycle.take(jobID)
		d.ack(msg, jobID)
		return
	}

	if attempt >= d.cfg.MaxAttempts {
		slog.Error("Giving up on status webhook",
			"job_id", jobID, "status", status.String(), "attempts", attempt)
		d.lifecycle.take(jobID)
		d.term(msg, jobID)
		return
	}

	delay := backoffFor(attempt, d.cfg.InitialBackoff, d.cfg.MaxBackoff)
	slog.Warn("Retrying status webhook",
		"job_id", jobID, "status", status.String(), "attempt", attempt, "retry_in", delay)
	if err := msg.NakWithDelay(delay); err != nil {
		slog.Warn("Failed to schedule status webhook retry", "job_id", jobID, "error", err)
	}
}

// deliver resolves the job's status callback URL and POSTs the event. It reports
// true when the event is settled - either delivered, or intentionally not sent
// because the job has no (valid) status callback URL. It reports false only when
// the attempt should be retried.
func (d *StatusWebhookDispatcher) deliver(ctx context.Context, msg statusMsg, jobID string, status buildstatus.JobStatus, finishedAt time.Time, attempt int) bool {
	sendCtx, cancel := context.WithTimeout(ctx, d.cfg.Timeout)
	defer cancel()

	if d.resolver == nil {
		return true
	}

	info, err := d.resolver.JobInfo(sendCtx, jobID)
	if err != nil {
		// A transient KV failure is worth retrying; it shares the attempt budget.
		slog.Warn("Failed to resolve status callback URL", "job_id", jobID, "error", err)
		return false
	}
	if !info.Found || info.StatusCallbackURL == "" {
		slog.Debug("No status callback URL for job, skipping webhook", "job_id", jobID)
		return true
	}
	if err := utils.ValidateCallbackURL(info.StatusCallbackURL); err != nil {
		// Retrying cannot fix a malformed URL, so settle it.
		slog.Warn("Invalid status_callback_url, skipping webhook", "job_id", jobID, "error", err)
		return true
	}

	queuedAt, startedAt := d.lifecycle.peek(jobID)
	reason := ""
	if headers := msg.Headers(); headers != nil {
		// Truncate here rather than trusting the publisher. The Docker scheduler
		// caps its reason, but the operator forwards a Kubernetes Job condition
		// message verbatim, so the cap only holds at the boundary that needs it -
		// this one, where the reason leaves the cluster in a webhook body.
		reason = buildstatus.TruncateReason(headers.Get(buildstatus.ReasonHeader))
	}

	event := buildEvent(jobID, info.Name, status, reason, queuedAt, startedAt, finishedAt, attempt)
	if err := d.sender.Send(sendCtx, info.StatusCallbackURL, event); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Debug("Status webhook cancelled during shutdown", "job_id", jobID)
		} else {
			slog.Warn("Status webhook delivery failed", "job_id", jobID, "attempt", attempt, "error", err)
		}
		return false
	}

	slog.Info("Delivered job status webhook",
		"job_id", jobID, "status", status.String(), "attempt", attempt)
	return true
}

func (d *StatusWebhookDispatcher) ack(msg statusMsg, jobID string) {
	if err := msg.Ack(); err != nil {
		slog.Warn("Failed to ack status event", "job_id", jobID, "error", err)
	}
}

func (d *StatusWebhookDispatcher) term(msg statusMsg, jobID string) {
	if err := msg.Term(); err != nil {
		slog.Warn("Failed to terminate status event", "job_id", jobID, "error", err)
	}
}
