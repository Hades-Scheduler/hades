package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	hadesnats "github.com/hades-scheduler/hades/shared/nats"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// natsImage is declared in resolver_test.go (same package).

// startNATS spins up a JetStream-enabled NATS container and returns a connection to it.
func startNATS(t *testing.T) *nats.Conn {
	t.Helper()

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        natsImage,
		ExposedPorts: []string{"4222/tcp", "8222/tcp"},
		Cmd:          []string{"-js", "-m", "8222"},
		WaitingFor:   wait.ForHTTP("/healthz").WithPort("8222/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "start NATS container")
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			slog.Warn("Could not terminate NATS container", "error", err)
		}
	})

	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err, "NATS endpoint")

	nc, err := hadesnats.SetupDefaultNatsConnection(hadesnats.ConnectionConfig{URL: "nats://" + endpoint})
	require.NoError(t, err, "connect to NATS")
	t.Cleanup(nc.Close)

	return nc
}

// TestStopWatchingDrainsAllContainerLogsBeforeCompletion reproduces the log-loss race:
// job logs travel over JetStream (durable, with latency) while the "succeeded" status
// travels over core NATS (instant). The operator publishes every container's log batch
// before announcing success, so by the time "succeeded" is handled the batches are all
// durably in the stream but not yet consumed. The previous behaviour hard-cancelled the
// watcher on "succeeded", abandoning the still-in-flight batches. This test publishes
// several containers' batches, then immediately drives the running -> succeeded path, and
// asserts the in-memory aggregation (which both the dashboard and the forward read) ends
// up with ALL containers' logs. It fails without the drain-before-forward fix and passes
// with it.
func TestStopWatchingDrainsAllContainerLogsBeforeCompletion(t *testing.T) {
	nc := startNATS(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer, err := buildlogs.NewHadesLogProducer(nc)
	require.NoError(t, err, "create log producer (also creates the stream)")

	consumer, err := buildlogs.NewHadesLogConsumer(nc)
	require.NoError(t, err, "create log consumer")

	// nil resolver: SendJobLogs is a no-op, we assert on the in-memory store.
	aggregator := NewLogAggregator(ctx, consumer, nil, nil, AggregatorConfig{
		BatchSize:  100,
		Retention:  time.Hour,
		MaxJobLogs: 1000,
	})

	dlm := &DynamicLogManager{
		nc:            nc,
		logConsumer:   consumer,
		logAggregator: aggregator,
		watchers:      make(map[string]watcherState),
		finalized:     make(map[string]time.Time),
	}

	const jobID = "race-job"
	containerIDs := []string{"buildjob-step-1", "buildjob-step-2", "buildjob-step-3", "buildjob-finalizer"}

	// Publish every container's log batch to JetStream first, mirroring the operator
	// publishing all logs before announcing "succeeded". js.Publish waits for the server
	// ack, so all batches are durably stored before we drive the status transitions.
	for i, cid := range containerIDs {
		err := producer.PublishJobLog(ctx, buildlogs.Log{
			JobID:       jobID,
			ContainerID: cid,
			Logs: []buildlogs.LogEntry{
				{Timestamp: time.Now(), Message: fmt.Sprintf("log for %s", cid), OutputStream: "stdout"},
				{Timestamp: time.Now(), Message: fmt.Sprintf("line 2 of container %d", i), OutputStream: "stdout"},
			},
		})
		require.NoError(t, err, "publish log for %s", cid)
	}

	// Drive the running -> succeeded path. Completion runs in the background, so wait
	// on sendWG to keep the assertions below deterministic.
	dlm.handleJobRunning(ctx, jobID)
	dlm.handleJobSucceeded(ctx, jobID)
	dlm.sendWG.Wait()

	logs := aggregator.GetJobLogs(jobID)

	got := make([]string, 0, len(logs))
	for _, l := range logs {
		got = append(got, l.ContainerID)
	}
	sort.Strings(got)

	want := append([]string(nil), containerIDs...)
	sort.Strings(want)

	require.ElementsMatch(t, want, got,
		"all container logs must be aggregated before the job is completed; got %v", got)

	for _, l := range logs {
		require.Len(t, l.Logs, 2, "each container's two log entries must survive the drain (container %s)", l.ContainerID)
	}
}

// TestTerminalWithoutWatcherReplaysOnce covers a terminal status for a job with no
// watcher (its Running was missed, or this process started mid-job): the logs are
// replayed from JetStream and forwarded exactly once, without blocking the caller.
func TestTerminalWithoutWatcherReplaysOnce(t *testing.T) {
	agg := newRecordingAggregator()
	dlm := newTestDLM(agg, &fakeLogConsumer{}, nil, time.Hour)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		dlm.handleJobSucceeded(ctx, "missed-running")
		dlm.handleJobSucceeded(ctx, "missed-running")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal handling blocked for a job without a watcher")
	}
	dlm.sendWG.Wait()

	events := agg.snapshot()
	require.Equal(t, 1, count(events, "sent:missed-running"), "a duplicate terminal must not resend")
	require.Less(t, slices.Index(events, "add:missed-running"), slices.Index(events, "sent:missed-running"),
		"the replay must finish before forwarding")
}

// A job that never ran (e.g. it failed while queued) has nothing in the stream,
// so nothing is forwarded for it.
func TestTerminalForJobThatNeverRanSendsNothing(t *testing.T) {
	agg := newRecordingAggregator()
	dlm := newTestDLM(agg, &fakeLogConsumer{noLogs: true}, nil, time.Hour)

	dlm.handleJobFailed(context.Background(), "never-ran")
	dlm.sendWG.Wait()
	require.Contains(t, agg.snapshot(), "completed:never-ran")
	require.NotContains(t, agg.snapshot(), "sent:never-ran")
}

// recordingAggregator records the order of calls and remembers which jobs have
// logs, so the "nothing to forward" check sees what the watchers added.
type recordingAggregator struct {
	mu      sync.Mutex
	events  []string
	hasLogs map[string]bool
}

func newRecordingAggregator() *recordingAggregator {
	return &recordingAggregator{hasLogs: make(map[string]bool)}
}

func (r *recordingAggregator) record(e string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recordingAggregator) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *recordingAggregator) AddLog(l buildlogs.Log) {
	r.record("add:" + l.JobID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hasLogs != nil {
		r.hasLogs[l.JobID] = true
	}
}
func (r *recordingAggregator) ResetJobLogs(id string) {
	r.record("reset:" + id)
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.hasLogs, id)
}
func (r *recordingAggregator) PrefetchJob(context.Context, string) (logTarget, bool) {
	return logTarget{}, false
}
func (r *recordingAggregator) FlushJob(string) error { return nil }
func (r *recordingAggregator) GetJobLogs(id string) []buildlogs.Log {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hasLogs[id] {
		return []buildlogs.Log{{JobID: id, ContainerID: "c"}}
	}
	return nil
}
func (r *recordingAggregator) GetAllJobs() []string { return nil }
func (r *recordingAggregator) UpdateJobStatus(id string, s buildstatus.JobStatus) {
	r.record("status:" + id + ":" + string(s))
}
func (r *recordingAggregator) GetJobStatus(string) (buildstatus.JobStatus, error) {
	return "", nil
}
func (r *recordingAggregator) MarkJobCompleted(id string) { r.record("completed:" + id) }
func (r *recordingAggregator) SendJobLogs(_ context.Context, id string) error {
	r.record("sent:" + id)
	return nil
}

// TestSlowDrainDoesNotBlockOtherJobs holds job A's drain open and checks that job B
// still gets completed and forwarded. A's logs must only go out after its drain ends.
func TestSlowDrainDoesNotBlockOtherJobs(t *testing.T) {
	agg := newRecordingAggregator()
	dlm := &DynamicLogManager{
		logAggregator: agg,
		watchers:      make(map[string]watcherState),
		finalized:     make(map[string]time.Time),
	}

	newWatcher := func() (watcherState, *sync.WaitGroup) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		return watcherState{cancel: func() {}, drain: make(chan struct{}), wg: wg}, wg
	}
	watcherA, wgA := newWatcher()
	watcherB, wgB := newWatcher()
	dlm.watchers["A"] = watcherA
	dlm.watchers["B"] = watcherB

	ctx := context.Background()

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		dlm.handleJobSucceeded(ctx, "A")
		dlm.handleJobSucceeded(ctx, "B")
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("handlers blocked on a slow drain")
	}

	// B's drain finishes while A's is still stuck.
	wgB.Done()
	require.Eventually(t, func() bool {
		return slices.Contains(agg.snapshot(), "sent:B")
	}, 2*time.Second, 5*time.Millisecond, "job B must be forwarded while A is still draining")
	require.NotContains(t, agg.snapshot(), "completed:A")
	require.NotContains(t, agg.snapshot(), "sent:A")

	// A completes: completed must precede sent.
	wgA.Done()
	dlm.sendWG.Wait()
	events := agg.snapshot()
	require.Less(t, slices.Index(events, "completed:A"), slices.Index(events, "sent:A"))
	require.Contains(t, events, "status:A:"+string(buildstatus.StatusSucceeded))

	// A duplicate terminal event is a no-op.
	dlm.handleJobSucceeded(ctx, "A")
	dlm.sendWG.Wait()
	require.Equal(t, 1, count(agg.snapshot(), "sent:A"))
}

func count(s []string, v string) int {
	n := 0
	for _, e := range s {
		if e == v {
			n++
		}
	}
	return n
}

// fakeLogConsumer stands in for the JetStream watcher: on drain it delivers one
// log (the job's stored backlog) unless noLogs is set, and returns. On cancel it
// returns only once release is closed (if set), to model a slow teardown.
type fakeLogConsumer struct {
	release chan struct{}
	noLogs  bool
}

func (f *fakeLogConsumer) WatchJobLogs(ctx context.Context, jobID string, drain <-chan struct{}, handler func(buildlogs.Log)) error {
	select {
	case <-drain:
		if !f.noLogs {
			handler(buildlogs.Log{JobID: jobID, ContainerID: "c"})
		}
		return nil
	case <-ctx.Done():
		if f.release != nil {
			<-f.release
		}
		return ctx.Err()
	}
}

func newTestDLM(agg watchedAggregator, consumer buildlogs.LogConsumer, nc *nats.Conn, maxLifetime time.Duration) *DynamicLogManager {
	return NewDynamicLogManager(nc, consumer, agg, LogWatcherConfig{MaxLifetime: maxLifetime}).(*DynamicLogManager)
}

// A fast job publishes Running and its terminal status back to back. Both must
// be handled in that order, or the watcher started by a late Running is never
// stopped and the job's logs are never forwarded.
func TestStatusEventsKeepPerJobOrder(t *testing.T) {
	nc := startNATS(t)
	agg := newRecordingAggregator()
	dlm := newTestDLM(agg, &fakeLogConsumer{}, nc, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	listening := make(chan error, 1)
	go func() { listening <- dlm.StartListening(ctx) }()
	// The subscription is registered asynchronously; a flush round-trip after a
	// short pause makes sure it exists before publishing.
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, nc.Flush())

	const jobs = 50
	for i := range jobs {
		id := fmt.Sprintf("job-%d", i)
		require.NoError(t, nc.Publish(buildstatus.StatusSubject(buildstatus.StatusRunning), []byte(id)))
		require.NoError(t, nc.Publish(buildstatus.StatusSubject(buildstatus.StatusSucceeded), []byte(id)))
	}
	require.NoError(t, nc.Flush())

	require.Eventually(t, func() bool {
		n := 0
		for _, e := range agg.snapshot() {
			if len(e) > 5 && e[:5] == "sent:" {
				n++
			}
		}
		return n == jobs
	}, 10*time.Second, 10*time.Millisecond, "every job must be forwarded")

	events := agg.snapshot()
	for i := range jobs {
		id := fmt.Sprintf("job-%d", i)
		running := slices.Index(events, "status:"+id+":"+string(buildstatus.StatusRunning))
		succeeded := slices.Index(events, "status:"+id+":"+string(buildstatus.StatusSucceeded))
		require.Less(t, running, succeeded, "Running must be handled before Succeeded for %s", id)
	}
	dlm.mu.RLock()
	require.Empty(t, dlm.watchers, "no watcher may outlive its job")
	dlm.mu.RUnlock()

	cancel()
	require.NoError(t, <-listening)
}

// A watcher that outlives its lifetime is stopped, but the job may still be
// running: nothing may be forwarded or completed until its terminal status,
// which then replays the full log and forwards it exactly once.
func TestWatcherExpiryDefersForwardToTerminal(t *testing.T) {
	agg := newRecordingAggregator()
	dlm := newTestDLM(agg, &fakeLogConsumer{}, nil, 50*time.Millisecond)
	ctx := context.Background()

	dlm.handleJobRunning(ctx, "long-job")
	require.Eventually(t, func() bool {
		dlm.mu.RLock()
		defer dlm.mu.RUnlock()
		return len(dlm.watchers) == 0
	}, 5*time.Second, 10*time.Millisecond, "the expired watcher must stop")
	require.NotContains(t, agg.snapshot(), "completed:long-job", "an expired job is not complete")
	require.NotContains(t, agg.snapshot(), "sent:long-job", "an expired watcher must not forward partial logs")

	dlm.handleJobSucceeded(ctx, "long-job")
	dlm.sendWG.Wait()
	events := agg.snapshot()
	require.Equal(t, 1, count(events, "sent:long-job"))
	require.Less(t, slices.Index(events, "reset:long-job"), slices.Index(events, "add:long-job"),
		"partial logs are dropped before the replay")
	require.Less(t, slices.Index(events, "add:long-job"), slices.Index(events, "sent:long-job"))
}

// A terminal status that arrives while the expired watcher is still winding
// down must still take the replay path, not drain the cancelled watcher.
func TestTerminalForExpiringWatcherReplays(t *testing.T) {
	release := make(chan struct{})
	agg := newRecordingAggregator()
	dlm := newTestDLM(agg, &fakeLogConsumer{release: release}, nil, time.Hour)
	ctx := context.Background()

	dlm.handleJobRunning(ctx, "job")
	dlm.mu.RLock()
	w := dlm.watchers["job"]
	dlm.mu.RUnlock()
	dlm.expireWatcher("job", w.drain, time.Hour)

	dlm.handleJobSucceeded(ctx, "job")
	require.NotContains(t, agg.snapshot(), "sent:job", "must wait for the expired watcher to finish")
	close(release)
	dlm.sendWG.Wait()

	events := agg.snapshot()
	require.Equal(t, 1, count(events, "sent:job"))
	require.Contains(t, events, "reset:job")
}

// The normal path drains the live watcher and does not replay.
func TestTerminalWithLiveWatcherDoesNotReplay(t *testing.T) {
	agg := newRecordingAggregator()
	dlm := newTestDLM(agg, &fakeLogConsumer{}, nil, time.Hour)
	ctx := context.Background()

	dlm.handleJobRunning(ctx, "job")
	dlm.handleJobSucceeded(ctx, "job")
	dlm.sendWG.Wait()

	events := agg.snapshot()
	require.Equal(t, 1, count(events, "add:job"), "logs are read once, by the live watcher")
	// The only reset is the watcher's own, before it reads; a replay would add a
	// second one after the drain.
	require.Equal(t, 1, count(events, "reset:job"))
	require.Less(t, slices.Index(events, "reset:job"), slices.Index(events, "add:job"))
	require.Equal(t, 1, count(events, "sent:job"))
}

// A Running for an already forwarded job (a redelivery) re-reads the subject
// from the start, so the previous run's aggregated logs must be dropped first.
func TestRunningAfterTerminalResetsLogs(t *testing.T) {
	agg := newRecordingAggregator()
	dlm := newTestDLM(agg, &fakeLogConsumer{}, nil, time.Hour)
	ctx := context.Background()

	dlm.handleJobRunning(ctx, "job")
	dlm.handleJobSucceeded(ctx, "job")
	dlm.sendWG.Wait()

	dlm.handleJobRunning(ctx, "job")
	dlm.handleJobSucceeded(ctx, "job")
	dlm.sendWG.Wait()

	events := agg.snapshot()
	require.Equal(t, 2, count(events, "reset:job"))
	require.Equal(t, 2, count(events, "sent:job"))
}

// The lifetime timer of a replaced watcher must not stop its replacement.
func TestExpiredWatcherDoesNotStopReplacement(t *testing.T) {
	agg := newRecordingAggregator()
	dlm := newTestDLM(agg, &fakeLogConsumer{}, nil, time.Hour)
	ctx := context.Background()

	dlm.handleJobRunning(ctx, "job")
	dlm.mu.RLock()
	first := dlm.watchers["job"]
	dlm.mu.RUnlock()

	dlm.handleJobRunning(ctx, "job")
	dlm.expireWatcher("job", first.drain, time.Hour)

	dlm.mu.RLock()
	current, stillWatched := dlm.watchers["job"]
	dlm.mu.RUnlock()
	require.True(t, stillWatched)
	require.False(t, current.expired, "the replacement must not be expired")
	require.NotContains(t, agg.snapshot(), "sent:job")

	dlm.handleJobSucceeded(ctx, "job")
	dlm.sendWG.Wait()
	require.Equal(t, 1, count(agg.snapshot(), "sent:job"))
}

func TestWatcherLifetime(t *testing.T) {
	require.Equal(t, 6*time.Hour, watcherLifetime(0, 6*time.Hour), "no timeout falls back to the ceiling")
	require.Equal(t, 90*time.Second+watcherLifetimeSlack, watcherLifetime(90, 6*time.Hour))
	require.Equal(t, staleJobTTL, watcherLifetime(1<<62, 6*time.Hour), "huge timeouts must not overflow")
}

// A redelivered Running replaces the job's watcher. Waiting for the old one to
// wind down must not block the status subscription, which serves every job.
func TestReplacingWatcherDoesNotBlockCaller(t *testing.T) {
	release := make(chan struct{})
	agg := newRecordingAggregator()
	dlm := newTestDLM(agg, &fakeLogConsumer{release: release}, nil, time.Hour)
	ctx := context.Background()

	dlm.handleJobRunning(ctx, "job")
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		dlm.handleJobRunning(ctx, "job")
		dlm.handleJobRunning(ctx, "other")
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("replacing a watcher blocked the caller")
	}

	close(release)
	dlm.handleJobSucceeded(ctx, "job")
	dlm.handleJobSucceeded(ctx, "other")
	dlm.sendWG.Wait()
	require.Equal(t, 1, count(agg.snapshot(), "sent:job"))
	require.Equal(t, 1, count(agg.snapshot(), "sent:other"))
}
