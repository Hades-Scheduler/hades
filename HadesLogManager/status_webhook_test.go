package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeMsg is a statusMsg standing in for a JetStream message.
type fakeMsg struct {
	subject string
	data    []byte
	headers nats.Header
	meta    *jetstream.MsgMetadata
	metaErr error

	mu        sync.Mutex
	acked     int
	termed    int
	nakDelays []time.Duration
}

func newFakeMsg(status buildstatus.JobStatus, jobID string, delivered uint64) *fakeMsg {
	return &fakeMsg{
		subject: buildstatus.StatusSubject(status),
		data:    []byte(jobID),
		meta: &jetstream.MsgMetadata{
			NumDelivered: delivered,
			Timestamp:    time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		},
	}
}

func (m *fakeMsg) Subject() string      { return m.subject }
func (m *fakeMsg) Data() []byte         { return m.data }
func (m *fakeMsg) Headers() nats.Header { return m.headers }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return m.meta, m.metaErr
}
func (m *fakeMsg) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked++
	return nil
}
func (m *fakeMsg) Term() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.termed++
	return nil
}
func (m *fakeMsg) NakWithDelay(delay time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nakDelays = append(m.nakDelays, delay)
	return nil
}
func (m *fakeMsg) naks() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]time.Duration(nil), m.nakDelays...)
}

// fakeInfoResolver returns a canned jobInfo.
type fakeInfoResolver struct {
	info jobInfo
	err  error
}

func (r fakeInfoResolver) JobInfo(context.Context, string) (jobInfo, error) {
	return r.info, r.err
}

// recordingSender captures every delivery attempt and can be told to fail.
type recordingSender struct {
	mu     sync.Mutex
	events []JobStatusEvent
	urls   []string
	err    error
}

func (s *recordingSender) Send(_ context.Context, url string, event JobStatusEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.urls = append(s.urls, url)
	s.events = append(s.events, event)
	return s.err
}

func (s *recordingSender) sent() []JobStatusEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]JobStatusEvent(nil), s.events...)
}

func testDispatcher(t *testing.T, resolver jobInfoResolver, sender webhookSender) *StatusWebhookDispatcher {
	t.Helper()
	return NewStatusWebhookDispatcher(nil, resolver, sender, StatusWebhookConfig{
		Enabled:        true,
		MaxAttempts:    3,
		Timeout:        time.Second,
		InitialBackoff: time.Second,
		MaxBackoff:     8 * time.Second,
		Concurrency:    2,
		MaxPending:     10,
	})
}

func webhookResolver(url string) fakeInfoResolver {
	return fakeInfoResolver{info: jobInfo{Found: true, Name: "Example Job", StatusCallbackURL: url}}
}

func TestBackoffForDoublesAndCaps(t *testing.T) {
	initial := time.Second
	maximum := 5 * time.Second

	assert.Equal(t, time.Second, backoffFor(1, initial, maximum))
	assert.Equal(t, 2*time.Second, backoffFor(2, initial, maximum))
	assert.Equal(t, 4*time.Second, backoffFor(3, initial, maximum))
	assert.Equal(t, maximum, backoffFor(4, initial, maximum), "growth is capped")
	assert.Equal(t, maximum, backoffFor(99, initial, maximum), "cap holds for large attempts")
	assert.Equal(t, initial, backoffFor(0, initial, maximum), "attempt is clamped to 1")
}

func TestHandleNonTerminalStatusRecordsTimestampsWithoutSending(t *testing.T) {
	sender := &recordingSender{}
	d := testDispatcher(t, webhookResolver("http://example.test/hook"), sender)

	queued := newFakeMsg(buildstatus.StatusQueued, "job-1", 1)
	queued.meta.Timestamp = time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	running := newFakeMsg(buildstatus.StatusRunning, "job-1", 1)
	running.meta.Timestamp = time.Date(2026, 8, 21, 12, 0, 5, 0, time.UTC)

	d.handle(context.Background(), queued)
	d.handle(context.Background(), running)

	assert.Equal(t, 1, queued.acked)
	assert.Equal(t, 1, running.acked)
	assert.Empty(t, sender.sent(), "non-terminal statuses never fire the webhook")

	q, s := d.lifecycle.peek("job-1")
	assert.Equal(t, queued.meta.Timestamp, q)
	assert.Equal(t, running.meta.Timestamp, s)
}

func TestHandleTerminalSuccessSendsWebhookAndAcks(t *testing.T) {
	sender := &recordingSender{}
	d := testDispatcher(t, webhookResolver("http://example.test/hook"), sender)

	queued := newFakeMsg(buildstatus.StatusQueued, "job-1", 1)
	queued.meta.Timestamp = time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	running := newFakeMsg(buildstatus.StatusRunning, "job-1", 1)
	running.meta.Timestamp = time.Date(2026, 8, 21, 12, 0, 5, 0, time.UTC)
	done := newFakeMsg(buildstatus.StatusSucceeded, "job-1", 1)
	done.meta.Timestamp = time.Date(2026, 8, 21, 12, 0, 11, 0, time.UTC)

	d.handle(context.Background(), queued)
	d.handle(context.Background(), running)
	d.handle(context.Background(), done)

	events := sender.sent()
	require.Len(t, events, 1)
	ev := events[0]
	assert.Equal(t, EventJobCompleted, ev.Event)
	assert.Equal(t, "job-1", ev.JobID)
	assert.Equal(t, "Example Job", ev.Name)
	assert.Equal(t, "Succeeded", ev.Status)
	assert.Empty(t, ev.Reason, "a successful job carries no reason")
	assert.Equal(t, 1, ev.Attempt)
	require.NotNil(t, ev.QueuedAt)
	require.NotNil(t, ev.StartedAt)
	assert.Equal(t, queued.meta.Timestamp, *ev.QueuedAt)
	assert.Equal(t, running.meta.Timestamp, *ev.StartedAt)
	assert.Equal(t, done.meta.Timestamp, ev.FinishedAt)
	require.NotNil(t, ev.DurationMs)
	assert.Equal(t, int64(6000), *ev.DurationMs)

	assert.Equal(t, 1, done.acked)
	assert.Empty(t, done.naks())
}

// TestHandleTerminalFailureIsDistinguishableFromSuccess is the property the
// existing log callback cannot provide: its body is a bare array of log lines,
// identical in shape whether the job passed or failed.
func TestHandleTerminalFailureIsDistinguishableFromSuccess(t *testing.T) {
	sender := &recordingSender{}
	d := testDispatcher(t, webhookResolver("http://example.test/hook"), sender)

	failed := newFakeMsg(buildstatus.StatusFailed, "job-2", 1)
	failed.headers = nats.Header{buildstatus.ReasonHeader: []string{"ImagePullBackOff: no such image"}}
	d.handle(context.Background(), failed)

	stopped := newFakeMsg(buildstatus.StatusStopped, "job-3", 1)
	d.handle(context.Background(), stopped)

	events := sender.sent()
	require.Len(t, events, 2)
	assert.Equal(t, "Failed", events[0].Status)
	assert.Equal(t, "ImagePullBackOff: no such image", events[0].Reason)
	assert.Equal(t, "Stopped", events[1].Status)
	assert.Empty(t, events[1].Reason)
	assert.Nil(t, events[0].StartedAt, "an unobserved Running event omits started_at")
	assert.Nil(t, events[0].DurationMs)
}

// TestHandleTruncatesOversizedReason pins that the cap is enforced here rather
// than assumed of the publisher: the operator forwards a Kubernetes Job
// condition message verbatim, so an uncapped reason does reach this code.
func TestHandleTruncatesOversizedReason(t *testing.T) {
	sender := &recordingSender{}
	d := testDispatcher(t, webhookResolver("http://example.test/hook"), sender)

	msg := newFakeMsg(buildstatus.StatusFailed, "job-9", 1)
	msg.headers = nats.Header{buildstatus.ReasonHeader: []string{strings.Repeat("x", buildstatus.MaxReasonLen*3)}}
	d.handle(context.Background(), msg)

	events := sender.sent()
	require.Len(t, events, 1)
	assert.Len(t, []rune(events[0].Reason), buildstatus.MaxReasonLen, "reason must be capped at the hard bound, ellipsis included")
}

func TestHandleRetriesWithBackoffThenGivesUp(t *testing.T) {
	sender := &recordingSender{err: errors.New("receiver down")}
	d := testDispatcher(t, webhookResolver("http://example.test/hook"), sender)

	// Attempt 1 and 2 are rescheduled with a growing delay.
	first := newFakeMsg(buildstatus.StatusFailed, "job-4", 1)
	d.handle(context.Background(), first)
	assert.Equal(t, []time.Duration{time.Second}, first.naks())
	assert.Zero(t, first.acked)
	assert.Zero(t, first.termed)

	second := newFakeMsg(buildstatus.StatusFailed, "job-4", 2)
	d.handle(context.Background(), second)
	assert.Equal(t, []time.Duration{2 * time.Second}, second.naks())

	// MaxAttempts is 3, so the third failure is terminated rather than retried
	// forever.
	third := newFakeMsg(buildstatus.StatusFailed, "job-4", 3)
	d.handle(context.Background(), third)
	assert.Empty(t, third.naks(), "no further retry once the budget is spent")
	assert.Equal(t, 1, third.termed)

	events := sender.sent()
	require.Len(t, events, 3)
	assert.Equal(t, []int{1, 2, 3}, []int{events[0].Attempt, events[1].Attempt, events[2].Attempt},
		"the attempt counter lets a receiver spot a redelivery")
}

func TestHandleWithoutStatusCallbackURLDoesNotSend(t *testing.T) {
	sender := &recordingSender{}
	// A job that only set callback_url: the log forwarding path still uses it,
	// the status webhook stays silent.
	d := testDispatcher(t, fakeInfoResolver{info: jobInfo{Found: true, Name: "Logs Only"}}, sender)

	msg := newFakeMsg(buildstatus.StatusSucceeded, "job-5", 1)
	d.handle(context.Background(), msg)

	assert.Empty(t, sender.sent())
	assert.Equal(t, 1, msg.acked)
}

func TestHandleUnknownJobIsAcked(t *testing.T) {
	sender := &recordingSender{}
	d := testDispatcher(t, fakeInfoResolver{}, sender)

	msg := newFakeMsg(buildstatus.StatusSucceeded, "job-6", 1)
	d.handle(context.Background(), msg)

	assert.Empty(t, sender.sent())
	assert.Equal(t, 1, msg.acked)
}

func TestHandleInvalidStatusCallbackURLIsNotRetried(t *testing.T) {
	sender := &recordingSender{}
	d := testDispatcher(t, webhookResolver("not-a-url"), sender)

	msg := newFakeMsg(buildstatus.StatusSucceeded, "job-7", 1)
	d.handle(context.Background(), msg)

	assert.Empty(t, sender.sent())
	assert.Equal(t, 1, msg.acked, "a malformed URL cannot be fixed by retrying")
}

func TestHandleResolverErrorIsRetried(t *testing.T) {
	sender := &recordingSender{}
	d := testDispatcher(t, fakeInfoResolver{err: errors.New("kv unavailable")}, sender)

	msg := newFakeMsg(buildstatus.StatusSucceeded, "job-8", 1)
	d.handle(context.Background(), msg)

	assert.Empty(t, sender.sent())
	assert.Equal(t, []time.Duration{time.Second}, msg.naks())
}

func TestHandleMalformedEventIsDiscarded(t *testing.T) {
	sender := &recordingSender{}
	d := testDispatcher(t, webhookResolver("http://example.test/hook"), sender)

	empty := newFakeMsg(buildstatus.StatusSucceeded, "", 1)
	d.handle(context.Background(), empty)
	assert.Equal(t, 1, empty.termed)

	unknown := newFakeMsg(buildstatus.JobStatus("Bogus"), "job-9", 1)
	d.handle(context.Background(), unknown)
	assert.Equal(t, 1, unknown.termed)

	assert.Empty(t, sender.sent())
}

func TestHTTPWebhookSenderPostsJSONWithHeaders(t *testing.T) {
	type received struct {
		body    []byte
		headers http.Header
	}
	got := make(chan received, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- received{body: body, headers: r.Header.Clone()}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	sender := newHTTPWebhookSender(5 * time.Second)
	event := buildEvent("job-1", "Example Job", buildstatus.StatusFailed, "boom",
		time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 21, 12, 0, 1, 0, time.UTC),
		time.Date(2026, 8, 21, 12, 0, 3, 0, time.UTC), 2)
	require.NoError(t, sender.Send(context.Background(), srv.URL, event))

	r := <-got
	assert.Equal(t, "application/json", r.headers.Get("Content-Type"))
	assert.Equal(t, EventJobCompleted, r.headers.Get(headerEvent))
	assert.Equal(t, "job-1", r.headers.Get(headerJobID))
	assert.Equal(t, "2", r.headers.Get(headerAttempt))

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(r.body, &decoded))
	assert.Equal(t, EventJobCompleted, decoded["event"])
	assert.Equal(t, "Failed", decoded["status"])
	assert.Equal(t, "boom", decoded["reason"])
	assert.Equal(t, float64(2), decoded["attempt"])
	assert.Equal(t, float64(2000), decoded["duration_ms"])
}

func TestHTTPWebhookSenderTreatsNon2xxAsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sender := newHTTPWebhookSender(5 * time.Second)
	err := sender.Send(context.Background(), srv.URL, JobStatusEvent{JobID: "job-1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestHTTPWebhookSenderDoesNotFollowRedirects(t *testing.T) {
	var redirectTargetHits int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&redirectTargetHits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer srv.Close()

	sender := newHTTPWebhookSender(5 * time.Second)
	err := sender.Send(context.Background(), srv.URL, JobStatusEvent{JobID: "job-1"})

	// Following the redirect would downgrade the POST to a bodyless GET and
	// report success, so a 3xx must surface as a delivery failure instead.
	require.Error(t, err)
	assert.Contains(t, err.Error(), "302")
	assert.Equal(t, int32(0), atomic.LoadInt32(&redirectTargetHits), "redirect target must not be contacted")
}

func TestLifecycleTrackerSweepsAndCaps(t *testing.T) {
	tracker := newLifecycleTracker()
	now := time.Now()

	tracker.observe("old", buildstatus.StatusQueued, now.Add(-48*time.Hour))
	tracker.observe("fresh", buildstatus.StatusQueued, now)
	tracker.sweep(now)

	_, _ = tracker.peek("fresh")
	tracker.mu.Lock()
	_, oldPresent := tracker.entries["old"]
	_, freshPresent := tracker.entries["fresh"]
	tracker.mu.Unlock()
	assert.False(t, oldPresent, "entries past the TTL are swept")
	assert.True(t, freshPresent)

	// take forgets the job so a completed job leaves no residue.
	tracker.take("fresh")
	tracker.mu.Lock()
	assert.Empty(t, tracker.entries)
	tracker.mu.Unlock()

	// The cap evicts the least recently updated entry.
	tracker.maxSize = 2
	tracker.observe("a", buildstatus.StatusQueued, now.Add(-3*time.Minute))
	tracker.observe("b", buildstatus.StatusQueued, now.Add(-2*time.Minute))
	tracker.observe("c", buildstatus.StatusQueued, now.Add(-time.Minute))
	tracker.mu.Lock()
	_, aPresent := tracker.entries["a"]
	tracker.mu.Unlock()
	assert.False(t, aPresent)
}

func TestStatusWebhookConfigNormalizesInvalidValues(t *testing.T) {
	cfg := StatusWebhookConfig{}.normalized()
	assert.Positive(t, cfg.MaxAttempts)
	assert.Positive(t, cfg.Timeout)
	assert.Positive(t, cfg.InitialBackoff)
	assert.GreaterOrEqual(t, cfg.MaxBackoff, cfg.InitialBackoff)
	assert.Positive(t, cfg.Concurrency)
	assert.GreaterOrEqual(t, cfg.MaxPending, cfg.Concurrency)
}

// TestStatusWebhookConfigFallbacksMatchEnvDefaults guards the one way the two
// halves of the config can drift apart: the envDefault tag is the value an
// operator reads as "the default", while normalized() is what they actually get
// when they set something invalid. envDefault tags are strings and cannot
// reference a Go constant, so nothing but this test keeps the two in step.
//
// Only the fields with an absolute fallback are checked. Enabled, MaxBackoff,
// and MaxPending are deliberately excluded: normalized() leaves Enabled alone so
// an explicit "false" survives, and clamps MaxBackoff to InitialBackoff and
// MaxPending to Concurrency, which are relative floors rather than defaults.
func TestStatusWebhookConfigFallbacksMatchEnvDefaults(t *testing.T) {
	fallbacks := StatusWebhookConfig{}.normalized()
	v := reflect.ValueOf(fallbacks)
	typ := v.Type()

	absolute := map[string]bool{
		"MaxAttempts":    true,
		"Timeout":        true,
		"InitialBackoff": true,
		"Concurrency":    true,
	}

	checked := 0
	for i := range typ.NumField() {
		field := typ.Field(i)
		if !absolute[field.Name] {
			continue
		}
		tag, ok := field.Tag.Lookup("envDefault")
		require.True(t, ok, "%s: expected an envDefault tag", field.Name)
		checked++

		switch want := v.Field(i).Interface().(type) {
		case int:
			parsed, err := strconv.Atoi(tag)
			require.NoError(t, err, "%s: unparseable envDefault %q", field.Name, tag)
			assert.Equal(t, parsed, want, "%s: envDefault and normalized() fallback disagree", field.Name)
		case time.Duration:
			parsed, err := time.ParseDuration(tag)
			require.NoError(t, err, "%s: unparseable envDefault %q", field.Name, tag)
			assert.Equal(t, parsed, want, "%s: envDefault and normalized() fallback disagree", field.Name)
		default:
			t.Fatalf("%s: envDefault tag on unhandled type %T", field.Name, want)
		}
	}
	assert.Len(t, absolute, checked, "a field with an absolute default was renamed or removed")
}
