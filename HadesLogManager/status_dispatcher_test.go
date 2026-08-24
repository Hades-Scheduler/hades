package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// webhookRecorder is an HTTP endpoint that records the status events posted to
// it. Its handler can be made slow to model a receiver that stops answering.
type webhookRecorder struct {
	server *httptest.Server

	mu     sync.Mutex
	events []JobStatusEvent
	status int
	block  chan struct{}
}

func newWebhookRecorder(t *testing.T) *webhookRecorder {
	t.Helper()
	rec := &webhookRecorder{status: http.StatusOK}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		block := rec.block
		status := rec.status
		rec.mu.Unlock()

		if block != nil {
			select {
			case <-block:
			case <-r.Context().Done():
				return
			}
		}

		body, _ := io.ReadAll(r.Body)
		var event JobStatusEvent
		if err := json.Unmarshal(body, &event); err == nil {
			rec.mu.Lock()
			rec.events = append(rec.events, event)
			rec.mu.Unlock()
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(rec.server.Close)
	return rec
}

func (r *webhookRecorder) url() string { return r.server.URL }

func (r *webhookRecorder) setStatus(status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = status
}

// blockUntilReleased makes the handler hang until the returned func is called.
func (r *webhookRecorder) blockUntilReleased(t *testing.T) func() {
	t.Helper()
	ch := make(chan struct{})
	r.mu.Lock()
	r.block = ch
	r.mu.Unlock()
	var once sync.Once
	release := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return release
}

func (r *webhookRecorder) received() []JobStatusEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]JobStatusEvent(nil), r.events...)
}

// statusWebhookHarness wires a dispatcher to a real NATS JetStream server.
type statusWebhookHarness struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	kv         jetstream.KeyValue
	dispatcher *StatusWebhookDispatcher
}

func startDispatcher(t *testing.T, ctx context.Context, cfg StatusWebhookConfig) *statusWebhookHarness {
	t.Helper()

	nc := startNATS(t)
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "HADES_JOBS"})
	require.NoError(t, err)

	dispatcher := NewStatusWebhookDispatcher(js, newKVCallbackResolver(kv), nil, cfg)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := dispatcher.Run(ctx); err != nil {
			t.Errorf("dispatcher stopped with error: %v", err)
		}
	}()
	t.Cleanup(func() { <-done })

	// The consumer uses DeliverNew, so wait for it to exist before publishing;
	// otherwise the test would race against consumer creation.
	require.Eventually(t, func() bool {
		_, err := js.Consumer(ctx, StatusStreamName, statusWebhookConsumer)
		return err == nil
	}, 30*time.Second, 50*time.Millisecond, "status webhook consumer never became ready")

	return &statusWebhookHarness{nc: nc, js: js, kv: kv, dispatcher: dispatcher}
}

// storeJob writes a job payload into the HADES_JOBS KV bucket, the same way
// HadesAPI does on enqueue.
func (h *statusWebhookHarness) storeJob(t *testing.T, job payload.QueuePayload) string {
	t.Helper()
	body, err := json.Marshal(job)
	require.NoError(t, err)
	_, err = h.kv.Put(context.Background(), job.ID.String(), body)
	require.NoError(t, err)
	return job.ID.String()
}

// publishStatus publishes a status transition over core NATS, exactly as the
// scheduler, the operator, and the API do today.
func (h *statusWebhookHarness) publishStatus(t *testing.T, status buildstatus.JobStatus, jobID string, reason string) {
	t.Helper()
	msg := &nats.Msg{Subject: buildstatus.StatusSubject(status), Data: []byte(jobID)}
	if reason != "" {
		msg.Header = nats.Header{buildstatus.ReasonHeader: []string{reason}}
	}
	require.NoError(t, h.nc.PublishMsg(msg))
	require.NoError(t, h.nc.Flush())
}

func fastWebhookConfig() StatusWebhookConfig {
	return StatusWebhookConfig{
		Enabled:        true,
		MaxAttempts:    3,
		Timeout:        2 * time.Second,
		InitialBackoff: 250 * time.Millisecond,
		MaxBackoff:     time.Second,
		Concurrency:    4,
		MaxPending:     32,
	}
}

// TestStatusWebhookDeliversTerminalStatus proves the core contract: a terminal
// status event published over core NATS reaches the job's status_callback_url
// with the outcome and lifecycle timestamps attached, without anything having to
// wait for the log consumer to drain.
func TestStatusWebhookDeliversTerminalStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness := startDispatcher(t, ctx, fastWebhookConfig())
	receiver := newWebhookRecorder(t)

	jobID := harness.storeJob(t, payload.QueuePayload{
		ID:                uuid.New(),
		Name:              "Example Job",
		StatusCallbackURL: receiver.url(),
	})

	harness.publishStatus(t, buildstatus.StatusQueued, jobID, "")
	harness.publishStatus(t, buildstatus.StatusRunning, jobID, "")
	harness.publishStatus(t, buildstatus.StatusSucceeded, jobID, "")

	require.Eventually(t, func() bool {
		return len(receiver.received()) == 1
	}, 30*time.Second, 50*time.Millisecond, "no status webhook received")

	event := receiver.received()[0]
	assert.Equal(t, EventJobCompleted, event.Event)
	assert.Equal(t, jobID, event.JobID)
	assert.Equal(t, "Example Job", event.Name)
	assert.Equal(t, "Succeeded", event.Status)
	assert.Equal(t, 1, event.Attempt)
	assert.False(t, event.FinishedAt.IsZero(), "finished_at is always present")
	require.NotNil(t, event.QueuedAt)
	require.NotNil(t, event.StartedAt)
	assert.False(t, event.StartedAt.Before(*event.QueuedAt))
	assert.False(t, event.FinishedAt.Before(*event.StartedAt))
}

// TestStatusWebhookDistinguishesFailureFromSuccess covers the gap that makes the
// existing log callback unusable as a completion signal: its body is a bare log
// array, so a receiver cannot tell a passing job from a failing one.
func TestStatusWebhookDistinguishesFailureFromSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness := startDispatcher(t, ctx, fastWebhookConfig())
	receiver := newWebhookRecorder(t)

	failedID := harness.storeJob(t, payload.QueuePayload{
		ID: uuid.New(), Name: "Failing Job", StatusCallbackURL: receiver.url(),
	})
	succeededID := harness.storeJob(t, payload.QueuePayload{
		ID: uuid.New(), Name: "Passing Job", StatusCallbackURL: receiver.url(),
	})

	harness.publishStatus(t, buildstatus.StatusFailed, failedID, "ImagePullBackOff: no such image")
	harness.publishStatus(t, buildstatus.StatusSucceeded, succeededID, "")

	require.Eventually(t, func() bool {
		return len(receiver.received()) == 2
	}, 30*time.Second, 50*time.Millisecond)

	byID := map[string]JobStatusEvent{}
	for _, event := range receiver.received() {
		byID[event.JobID] = event
	}

	require.Contains(t, byID, failedID)
	assert.Equal(t, "Failed", byID[failedID].Status)
	assert.Equal(t, "ImagePullBackOff: no such image", byID[failedID].Reason)

	require.Contains(t, byID, succeededID)
	assert.Equal(t, "Succeeded", byID[succeededID].Status)
	assert.Empty(t, byID[succeededID].Reason)
}

// TestStatusWebhookRetriesThenGivesUp asserts the at-least-once behaviour end to
// end: a receiver that answers 5xx is retried with a growing attempt counter and
// is eventually abandoned instead of being retried forever.
func TestStatusWebhookRetriesThenGivesUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness := startDispatcher(t, ctx, fastWebhookConfig())
	receiver := newWebhookRecorder(t)
	receiver.setStatus(http.StatusInternalServerError)

	jobID := harness.storeJob(t, payload.QueuePayload{
		ID: uuid.New(), Name: "Retried Job", StatusCallbackURL: receiver.url(),
	})
	harness.publishStatus(t, buildstatus.StatusFailed, jobID, "boom")

	require.Eventually(t, func() bool {
		return len(receiver.received()) == 3
	}, 30*time.Second, 50*time.Millisecond, "expected MaxAttempts deliveries")

	attempts := []int{}
	for _, event := range receiver.received() {
		attempts = append(attempts, event.Attempt)
		assert.Equal(t, jobID, event.JobID, "every redelivery keeps the same dedupe key")
	}
	assert.Equal(t, []int{1, 2, 3}, attempts)

	// The budget is spent: nothing more arrives.
	assert.Never(t, func() bool {
		return len(receiver.received()) > 3
	}, 3*time.Second, 200*time.Millisecond)
}

// TestStatusWebhookDeadReceiverDoesNotBlockOtherJobs pins the isolation
// requirement: a receiver that never answers must not delay another job's
// notification.
func TestStatusWebhookDeadReceiverDoesNotBlockOtherJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := fastWebhookConfig()
	cfg.Timeout = 20 * time.Second // long enough that a blocked send would stall a serial dispatcher
	harness := startDispatcher(t, ctx, cfg)

	dead := newWebhookRecorder(t)
	release := dead.blockUntilReleased(t)
	alive := newWebhookRecorder(t)

	deadID := harness.storeJob(t, payload.QueuePayload{
		ID: uuid.New(), Name: "Dead Receiver Job", StatusCallbackURL: dead.url(),
	})
	aliveID := harness.storeJob(t, payload.QueuePayload{
		ID: uuid.New(), Name: "Healthy Receiver Job", StatusCallbackURL: alive.url(),
	})

	// The dead receiver's job is published first, so a dispatcher that delivered
	// serially would hold the healthy job behind it.
	harness.publishStatus(t, buildstatus.StatusSucceeded, deadID, "")
	harness.publishStatus(t, buildstatus.StatusSucceeded, aliveID, "")

	require.Eventually(t, func() bool {
		return len(alive.received()) == 1
	}, 10*time.Second, 50*time.Millisecond, "healthy receiver was blocked behind a dead one")
	assert.Equal(t, aliveID, alive.received()[0].JobID)
	assert.Empty(t, dead.received(), "the dead receiver is still hanging")

	release()
}

// TestStatusWebhookIgnoresJobsWithoutStatusCallback asserts that existing
// callback_url-only jobs are untouched: they get no status webhook, and their
// log forwarding configuration is not repurposed as a webhook destination.
func TestStatusWebhookIgnoresJobsWithoutStatusCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness := startDispatcher(t, ctx, fastWebhookConfig())
	logReceiver := newWebhookRecorder(t)
	statusReceiver := newWebhookRecorder(t)

	legacyID := harness.storeJob(t, payload.QueuePayload{
		ID: uuid.New(), Name: "Legacy Job", CallbackURL: logReceiver.url(),
	})
	optedInID := harness.storeJob(t, payload.QueuePayload{
		ID: uuid.New(), Name: "Opted In Job", StatusCallbackURL: statusReceiver.url(),
	})

	harness.publishStatus(t, buildstatus.StatusSucceeded, legacyID, "")
	harness.publishStatus(t, buildstatus.StatusSucceeded, optedInID, "")

	require.Eventually(t, func() bool {
		return len(statusReceiver.received()) == 1
	}, 30*time.Second, 50*time.Millisecond)

	assert.Never(t, func() bool {
		return len(logReceiver.received()) > 0
	}, 2*time.Second, 200*time.Millisecond,
		"a job that only set callback_url must not receive a status webhook")
}

// TestStatusWebhookStreamDoesNotDisturbCoreSubscribers guards the compatibility
// promise of capturing the status subjects in a JetStream stream: publishers
// keep using core NATS and existing core subscribers (log watching, the
// dashboard feed) keep receiving every event.
func TestStatusWebhookStreamDoesNotDisturbCoreSubscribers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness := startDispatcher(t, ctx, fastWebhookConfig())

	seen := make(chan string, 4)
	sub, err := harness.nc.Subscribe(buildstatus.StatusSubject("*"), func(msg *nats.Msg) {
		seen <- msg.Subject + "|" + string(msg.Data)
	})
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()
	require.NoError(t, harness.nc.Flush())

	jobID := uuid.New().String()
	harness.publishStatus(t, buildstatus.StatusRunning, jobID, "")
	harness.publishStatus(t, buildstatus.StatusSucceeded, jobID, "")

	for _, want := range []string{
		buildstatus.StatusSubject(buildstatus.StatusRunning) + "|" + jobID,
		buildstatus.StatusSubject(buildstatus.StatusSucceeded) + "|" + jobID,
	} {
		select {
		case got := <-seen:
			assert.Equal(t, want, got)
		case <-time.After(10 * time.Second):
			t.Fatalf("core subscriber never received %s", want)
		}
	}
}
