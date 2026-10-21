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
	aggregator := NewLogAggregator(ctx, consumer, nil, AggregatorConfig{
		BatchSize:  100,
		Retention:  time.Hour,
		MaxJobLogs: 1000,
	})

	dlm := &DynamicLogManager{
		nc:            nc,
		logConsumer:   consumer,
		logAggregator: aggregator,
		watchers:      make(map[string]watcherState),
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

// TestStopWatchingWithoutWatcherIsNoop guards the case where a terminal status arrives
// for a job that was never running: stopWatchingJobLogs must not panic or block.
func TestStopWatchingWithoutWatcherIsNoop(t *testing.T) {
	dlm := &DynamicLogManager{
		watchers: make(map[string]watcherState),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		dlm.stopWatchingJobLogs(context.Background(), "unknown-job")
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stopWatchingJobLogs blocked for an unknown job")
	}
}

// recordingAggregator records the order of completion calls and otherwise does nothing.
type recordingAggregator struct {
	mu     sync.Mutex
	events []string
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

func (r *recordingAggregator) AddLog(buildlogs.Log)              {}
func (r *recordingAggregator) FlushJob(string) error             { return nil }
func (r *recordingAggregator) GetJobLogs(string) []buildlogs.Log { return nil }
func (r *recordingAggregator) GetAllJobs() []string              { return nil }
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
	agg := &recordingAggregator{}
	dlm := &DynamicLogManager{
		logAggregator: agg,
		watchers:      make(map[string]watcherState),
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
