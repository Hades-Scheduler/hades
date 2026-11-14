package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeResolver is a test double for jobCallbackResolver.
type fakeResolver struct {
	url string
	err error
}

func (f fakeResolver) LogTarget(ctx context.Context, jobID string) (logTarget, error) {
	return logTarget{CallbackURL: f.url}, f.err
}

func testConfig() AggregatorConfig {
	return AggregatorConfig{BatchSize: 100, Retention: time.Hour, MaxJobLogs: 1000}
}

func newTestAggregator(t *testing.T, resolver jobCallbackResolver) buildlogs.LogAggregator {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return NewLogAggregator(ctx, nil, resolver, loopbackClient(t), testConfig())
}

// newTestAggregatorMax builds an aggregator with a specific per-container entry
// cap (MaxJobLogs) and no callback resolver, for the coalescing/trim tests.
func newTestAggregatorMax(t *testing.T, maxJobLogs int) buildlogs.LogAggregator {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := AggregatorConfig{BatchSize: 100, Retention: time.Hour, MaxJobLogs: maxJobLogs}
	return NewLogAggregator(ctx, nil, nil, nil, cfg)
}

func logEntry(msg string) buildlogs.LogEntry {
	return buildlogs.LogEntry{Timestamp: time.Unix(0, 0), Message: msg, OutputStream: "stdout"}
}

// Streaming delivers many small Logs per container. They must coalesce into one
// element per container, in first-seen order, so the Artemis adapter's
// positional logs[1] == execute step contract holds.
func TestAddLog_CoalescesByContainerInOrder(t *testing.T) {
	agg := newTestAggregatorMax(t, 1000)

	agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "step-1", Logs: []buildlogs.LogEntry{logEntry("clone 1")}})
	agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "step-2", Logs: []buildlogs.LogEntry{logEntry("exec 1")}})
	agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "step-1", Logs: []buildlogs.LogEntry{logEntry("clone 2")}})
	agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "step-2", Logs: []buildlogs.LogEntry{logEntry("exec 2")}})
	agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "step-3", Logs: []buildlogs.LogEntry{logEntry("result 1")}})

	logs := agg.GetJobLogs("job-1")
	if len(logs) != 3 {
		t.Fatalf("expected one Log per container (3), got %d", len(logs))
	}

	want := []struct {
		container string
		messages  []string
	}{
		{"step-1", []string{"clone 1", "clone 2"}},
		{"step-2", []string{"exec 1", "exec 2"}},
		{"step-3", []string{"result 1"}},
	}
	for i, w := range want {
		if logs[i].ContainerID != w.container {
			t.Fatalf("logs[%d] container = %q, want %q", i, logs[i].ContainerID, w.container)
		}
		if len(logs[i].Logs) != len(w.messages) {
			t.Fatalf("logs[%d] entries = %d, want %d", i, len(logs[i].Logs), len(w.messages))
		}
		for j, m := range w.messages {
			if logs[i].Logs[j].Message != m {
				t.Fatalf("logs[%d].Logs[%d] = %q, want %q", i, j, logs[i].Logs[j].Message, m)
			}
		}
	}
}

// A step that produces no output must still occupy its slot so later steps do
// not shift into its index.
func TestAddLog_ZeroEntryLogRegistersSlot(t *testing.T) {
	agg := newTestAggregatorMax(t, 1000)

	agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "step-1", Logs: nil}) // clone: no output
	agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "step-2", Logs: []buildlogs.LogEntry{logEntry("exec")}})

	logs := agg.GetJobLogs("job-1")
	if len(logs) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(logs))
	}
	if logs[0].ContainerID != "step-1" || len(logs[0].Logs) != 0 {
		t.Fatalf("logs[0] = %+v, want empty step-1 slot", logs[0])
	}
	if logs[1].ContainerID != "step-2" {
		t.Fatalf("logs[1] container = %q, want step-2 (execute must stay at index 1)", logs[1].ContainerID)
	}
}

// Trimming caps entries within a container and must never drop a whole
// container element (which would shift step indices).
func TestAddLog_TrimsWithinContainerNotAcrossContainers(t *testing.T) {
	agg := newTestAggregatorMax(t, 3)

	for i := 0; i < 10; i++ {
		agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "step-1", Logs: []buildlogs.LogEntry{logEntry(fmt.Sprintf("a%d", i))}})
	}
	agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "step-2", Logs: []buildlogs.LogEntry{logEntry("b0")}})

	logs := agg.GetJobLogs("job-1")
	if len(logs) != 2 {
		t.Fatalf("expected 2 containers preserved, got %d", len(logs))
	}
	if len(logs[0].Logs) != 3 {
		t.Fatalf("step-1 entries = %d, want capped at 3", len(logs[0].Logs))
	}
	// The tail (newest) entries are kept.
	if logs[0].Logs[0].Message != "a7" || logs[0].Logs[2].Message != "a9" {
		t.Fatalf("step-1 kept wrong entries: %q..%q, want a7..a9", logs[0].Logs[0].Message, logs[0].Logs[2].Message)
	}
	if logs[1].ContainerID != "step-2" {
		t.Fatalf("step-2 slot lost after trimming step-1")
	}
}

// The copy-on-write merge must not mutate a slice a concurrent reader holds.
// Run with -race to catch aliasing regressions.
func TestAddLog_ConcurrentReadersNoRace(t *testing.T) {
	agg := newTestAggregatorMax(t, 100000)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for _, l := range agg.GetJobLogs("job-1") {
					for _, e := range l.Logs {
						_ = e.Message
					}
				}
			}
		}
	}()

	for i := 0; i < 2000; i++ {
		agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "step-1", Logs: []buildlogs.LogEntry{logEntry(fmt.Sprintf("m%d", i))}})
	}
	close(stop)
	wg.Wait()
}

func TestSendJobLogs_ForwardsToResolvedURL(t *testing.T) {
	var hits int32
	var received []buildlogs.Log
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &received)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	agg := newTestAggregator(t, fakeResolver{url: srv.URL})
	agg.AddLog(buildlogs.Log{
		JobID:       "job-1",
		ContainerID: "c1",
		Logs:        []buildlogs.LogEntry{{Timestamp: time.Now(), Message: "hello", OutputStream: "stdout"}},
	})

	require.NoError(t, agg.SendJobLogs(context.Background(), "job-1"))
	assert.Equal(t, int32(1), atomic.LoadInt32(&hits))
	require.Len(t, received, 1)
	assert.Equal(t, "job-1", received[0].JobID)
}

func TestSendJobLogs_NoCallbackURL_IsNoop(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	agg := newTestAggregator(t, fakeResolver{url: ""})
	agg.AddLog(buildlogs.Log{JobID: "job-1"})

	require.NoError(t, agg.SendJobLogs(context.Background(), "job-1"))
	assert.Equal(t, int32(0), atomic.LoadInt32(&hits), "no request should be sent when no callback URL is resolved")
}

func TestSendJobLogs_InvalidCallbackURL_IsNoop(t *testing.T) {
	agg := newTestAggregator(t, fakeResolver{url: "not-a-valid-url"})
	agg.AddLog(buildlogs.Log{JobID: "job-1"})

	// Invalid URL is skipped, not treated as a delivery failure.
	require.NoError(t, agg.SendJobLogs(context.Background(), "job-1"))
}

func TestSendJobLogs_ResolverError_IsNoop(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	agg := newTestAggregator(t, fakeResolver{url: srv.URL, err: assertErr("kv unavailable")})
	agg.AddLog(buildlogs.Log{JobID: "job-1"})

	require.NoError(t, agg.SendJobLogs(context.Background(), "job-1"))
	assert.Equal(t, int32(0), atomic.LoadInt32(&hits))
}

func TestSendJobLogs_NilResolver_IsNoop(t *testing.T) {
	agg := newTestAggregator(t, nil)
	agg.AddLog(buildlogs.Log{JobID: "job-1"})

	require.NoError(t, agg.SendJobLogs(context.Background(), "job-1"))
}

// assertErr is a tiny error helper to avoid importing errors just for a literal.
type assertErr string

func (e assertErr) Error() string { return string(e) }

// countingResolver counts lookups so tests can tell a cached target from a
// fresh KV read.
type countingResolver struct {
	target logTarget
	calls  atomic.Int32
}

func (c *countingResolver) LogTarget(context.Context, string) (logTarget, error) {
	c.calls.Add(1)
	return c.target, nil
}

// The callback URL is resolved when the watcher starts, so the forward after the
// terminal status does not wait on a KV read.
func TestSendJobLogs_UsesPrefetchedTarget(t *testing.T) {
	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	resolver := &countingResolver{target: logTarget{CallbackURL: srv.URL, TimeoutSeconds: 30}}
	agg := newTestAggregator(t, resolver).(*NATSLogAggregator)

	target, ok := agg.PrefetchJob(context.Background(), "job-1")
	require.True(t, ok)
	assert.Equal(t, int64(30), target.TimeoutSeconds)

	require.NoError(t, agg.SendJobLogs(context.Background(), "job-1"))
	assert.Equal(t, int32(1), resolver.calls.Load(), "SendJobLogs must reuse the prefetched target")
	assert.Equal(t, int32(1), received.Load())

	// Without a prefetch the target is still resolved at send time.
	require.NoError(t, agg.SendJobLogs(context.Background(), "job-2"))
	assert.Equal(t, int32(2), resolver.calls.Load())
}

// A job that never completes must not stay in memory forever.
func TestCleanupStaleJobs(t *testing.T) {
	agg := newTestAggregator(t, nil).(*NATSLogAggregator)
	agg.UpdateJobStatus("stuck", "Running")
	agg.AddLog(buildlogs.Log{JobID: "stuck", ContainerID: "c", Logs: []buildlogs.LogEntry{logEntry("x")}})
	agg.UpdateJobStatus("done", "Succeeded")
	agg.MarkJobCompleted("done")

	agg.cleanupStaleJobs(time.Now().Add(staleJobTTL + time.Minute))

	_, err := agg.GetJobStatus("stuck")
	assert.Error(t, err, "a job stuck past the TTL is flushed")
	assert.Empty(t, agg.GetJobLogs("stuck"))
	_, err = agg.GetJobStatus("done")
	assert.NoError(t, err, "completed jobs are left to the retention sweep")

	agg.UpdateJobStatus("fresh", "Running")
	agg.cleanupStaleJobs(time.Now())
	_, err = agg.GetJobStatus("fresh")
	assert.NoError(t, err)
}

func TestSendJobLogs_DoesNotFollowRedirects(t *testing.T) {
	var targetHits int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&targetHits, 1)
	}))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()

	agg := newTestAggregator(t, fakeResolver{url: srv.URL})
	agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "c1"})

	require.Error(t, agg.SendJobLogs(context.Background(), "job-1"))
	assert.Equal(t, int32(0), atomic.LoadInt32(&targetHits), "redirect target must not be contacted")
}

func TestSendJobLogs_BlocksLoopbackByDefault(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	agg := NewLogAggregator(ctx, nil, fakeResolver{url: srv.URL}, nil, testConfig())
	agg.AddLog(buildlogs.Log{JobID: "job-1", ContainerID: "c1"})

	require.ErrorIs(t, agg.SendJobLogs(context.Background(), "job-1"), errBlockedTarget)
	assert.Equal(t, int32(0), atomic.LoadInt32(&hits))
}

func newBudgetAggregator(t *testing.T, budget int) *NATSLogAggregator {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := AggregatorConfig{BatchSize: 100, Retention: time.Hour, MaxJobLogs: 1000, MaxTotalBytes: budget}
	return NewLogAggregator(ctx, nil, nil, nil, cfg)
}

func addBytes(agg *NATSLogAggregator, jobID string, n int) {
	agg.AddLog(buildlogs.Log{JobID: jobID, ContainerID: "c", Logs: []buildlogs.LogEntry{logEntry(strings.Repeat("x", n))}})
}

func completeJob(agg *NATSLogAggregator, jobID string, at time.Time) {
	agg.UpdateJobStatus(jobID, "Succeeded")
	agg.completed.Store(jobID, at)
}

func isEvicted(agg *NATSLogAggregator, jobID string) bool {
	l := agg.GetJobLogs(jobID)
	return len(l) == 1 && l[0].ContainerID == evictedContainerID && l[0].Logs[0].Message == evictedMessage
}

func TestLogBudget_EvictsOldestCompletedFirst(t *testing.T) {
	agg := newBudgetAggregator(t, 100)
	now := time.Now()
	addBytes(agg, "old", 40)
	addBytes(agg, "new", 40)
	completeJob(agg, "new", now)
	completeJob(agg, "old", now.Add(-time.Minute))

	addBytes(agg, "run", 40) // 120 > 100: only the oldest completed job must go

	assert.True(t, isEvicted(agg, "old"))
	assert.False(t, isEvicted(agg, "new"))
	assert.Equal(t, 80, agg.totalBytes)
	assert.LessOrEqual(t, agg.totalBytes, 100)
}

func TestLogBudget_KeepsRunningLogsAndStatusAfterEviction(t *testing.T) {
	agg := newBudgetAggregator(t, 100)
	addBytes(agg, "run", 50)
	addBytes(agg, "done", 40)
	completeJob(agg, "done", time.Now())

	addBytes(agg, "run", 30)

	assert.True(t, isEvicted(agg, "done"), "the eviction marker is visible instead of an empty log")
	s, err := agg.GetJobStatus("done")
	assert.NoError(t, err)
	assert.Equal(t, buildstatus.JobStatus("Succeeded"), s)
	logs := agg.GetJobLogs("run")
	assert.Len(t, logs, 1)
	assert.Len(t, logs[0].Logs, 2, "running job keeps all its lines")
}

func TestLogBudget_DisabledWithZero(t *testing.T) {
	agg := newBudgetAggregator(t, 0)
	addBytes(agg, "a", 1000)
	completeJob(agg, "a", time.Now())
	addBytes(agg, "b", 1000)
	assert.False(t, isEvicted(agg, "a"))
	assert.Len(t, agg.GetJobLogs("b")[0].Logs, 1)
}

func TestLogBudget_DropsNewLinesWhenOnlyRunningJobsHoldBudget(t *testing.T) {
	agg := newBudgetAggregator(t, 100)
	addBytes(agg, "r1", 80)
	addBytes(agg, "r2", 40) // does not fit, nothing completed to evict
	addBytes(agg, "r2", 40)

	assert.Equal(t, 80, agg.totalBytes)
	logs := agg.GetJobLogs("r2")
	assert.Len(t, logs, 1, "the container slot is still registered")
	assert.Empty(t, logs[0].Logs)
	assert.Len(t, agg.GetJobLogs("r1")[0].Logs, 1)
}

func TestLogBudget_FlushAndResetReleaseBytes(t *testing.T) {
	agg := newBudgetAggregator(t, 100)
	addBytes(agg, "a", 30)
	addBytes(agg, "b", 30)
	assert.Equal(t, 60, agg.totalBytes)
	agg.ResetJobLogs("a")
	assert.NoError(t, agg.FlushJob("b"))
	assert.Equal(t, 0, agg.totalBytes)
}

func TestLogBudget_ConcurrentAddsNoRaceAndWithinBudget(t *testing.T) {
	agg := newBudgetAggregator(t, 500)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("job-%d", i)
			for j := 0; j < 50; j++ {
				addBytes(agg, id, 10)
			}
			completeJob(agg, id, time.Now())
		}(i)
	}
	wg.Wait()
	agg.mu.Lock()
	defer agg.mu.Unlock()
	assert.LessOrEqual(t, agg.totalBytes, 500)
}

// A container at its MaxJobLogs cap replaces its oldest line, so the new line
// must not count as extra budget even when the budget is full.
func TestLogBudget_CappedContainerReplacesOldestLine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	agg := NewLogAggregator(ctx, nil, nil, nil, AggregatorConfig{BatchSize: 100, Retention: time.Hour, MaxJobLogs: 1, MaxTotalBytes: 100})

	agg.AddLog(buildlogs.Log{JobID: "j", ContainerID: "c", Logs: []buildlogs.LogEntry{logEntry(strings.Repeat("a", 60))}})
	agg.AddLog(buildlogs.Log{JobID: "j", ContainerID: "c", Logs: []buildlogs.LogEntry{logEntry(strings.Repeat("b", 60))}})

	logs := agg.GetJobLogs("j")
	require.Len(t, logs[0].Logs, 1)
	assert.Equal(t, strings.Repeat("b", 60), logs[0].Logs[0].Message)
	assert.Equal(t, 60, agg.totalBytes)
}

// Flush and reset race with AddLog: the byte accounting must end at zero once
// the logs are gone.
func TestLogBudget_FlushRaceLeavesNoAccounting(t *testing.T) {
	agg := newBudgetAggregator(t, 1<<20)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			addBytes(agg, "j", 10)
		}
	}()
	for i := 0; i < 200; i++ {
		_ = agg.FlushJob("j")
		agg.ResetJobLogs("j")
	}
	<-done
	_ = agg.FlushJob("j")

	assert.Equal(t, 0, agg.totalBytes)
	assert.Empty(t, agg.jobBytes)
}
