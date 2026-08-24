package docker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hades-scheduler/hades/hadesScheduler/log"
	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/hades-scheduler/hades/shared/timing"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
)

// recordingTracer captures the phases recorded during a job so a test can assert
// the executor instrumented every expected boundary.
type recordingTracer struct {
	mu  sync.Mutex
	got map[timing.Phase]bool
}

func (r *recordingTracer) StartJob(ctx context.Context, _, _ string, _ time.Time) (context.Context, func()) {
	return ctx, func() {}
}

func (r *recordingTracer) Phase(_ context.Context, phase timing.Phase, _, _ time.Time) {
	r.mu.Lock()
	if r.got == nil {
		r.got = map[timing.Phase]bool{}
	}
	r.got[phase] = true
	r.mu.Unlock()
}

func (r *recordingTracer) has(p timing.Phase) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.got[p]
}

// capturingPublisher records every Log handed to it so tests can assert on the
// output that was demultiplexed from the container's log stream.
type capturingPublisher struct {
	mu   sync.Mutex
	logs []buildlogs.Log
}

func (c *capturingPublisher) PublishJobLog(_ context.Context, jobLog buildlogs.Log) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logs = append(c.logs, jobLog)
	return nil
}

// messages returns every log line published across all containers.
func (c *capturingPublisher) messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []string
	for _, jobLog := range c.logs {
		for _, entry := range jobLog.Logs {
			out = append(out, entry.Message)
		}
	}
	return out
}

// newTestScheduler builds a Scheduler against the daemon described by the
// environment, skipping the test when no Docker daemon is reachable.
func newTestScheduler(t *testing.T, publisher buildlogs.LogPublisher) *Scheduler {
	t.Helper()

	cli, err := client.New(client.FromEnv)
	if err != nil {
		t.Skipf("no Docker client available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
		t.Skipf("no Docker daemon reachable: %v", err)
	}

	return &Scheduler{
		cli: cli,
		Options: Options{
			// alpine ships /bin/sh, not bash.
			scriptExecutor:      "/bin/sh -c",
			containerAutoremove: false,
		},
		logPublisher:    publisher,
		statusPublisher: log.NewNoopPublisher(),
	}
}

const testImage = "alpine:3.22"

// TestScheduleJobSharesVolumeBetweenSteps runs a real two-step job end to end.
// It covers every Docker API call the executor makes: volume create/remove,
// image pull, container create/start/wait/logs/remove - and asserts that the
// per-job shared volume actually carries state from one step to the next.
func TestScheduleJobSharesVolumeBetweenSteps(t *testing.T) {
	publisher := &capturingPublisher{}
	scheduler := newTestScheduler(t, publisher)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	job := payload.QueuePayload{
		ID:   uuid.New(),
		Name: "shared-volume-job",
		Steps: []payload.Step{
			{ID: 1, Image: testImage, Script: "echo hello-from-step-one > /shared/marker"},
			{ID: 2, Image: testImage, Script: "cat /shared/marker"},
		},
	}

	require.NoError(t, scheduler.ScheduleJob(ctx, job))

	// Step two can only print the marker if it saw step one's write, so this
	// asserts both the shared volume and the log demultiplexing path.
	require.Contains(t, strings.Join(publisher.messages(), "\n"), "hello-from-step-one")
}

// TestScheduleJobRecordsPhases asserts the executor records every phase of the
// taxonomy for a successful step, from queue_wait through teardown, so the
// overhead/runtime breakdown is complete.
func TestScheduleJobRecordsPhases(t *testing.T) {
	rec := &recordingTracer{}
	timing.SetTracer(rec)
	t.Cleanup(func() { timing.SetTracer(nil) })

	scheduler := newTestScheduler(t, &capturingPublisher{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	job := payload.QueuePayload{
		ID:        uuid.New(),
		Name:      "phase-coverage-job",
		Timestamp: time.Now().Add(-10 * time.Millisecond), // so queue_wait is recorded
		Steps: []payload.Step{
			{ID: 1, Image: testImage, Script: "echo hi"},
		},
	}

	require.NoError(t, scheduler.ScheduleJob(ctx, job))

	// queue_wait is intentionally not emitted as a span (it precedes the root
	// span); it is still logged and metered. Every other phase is a span.
	for _, p := range []timing.Phase{
		timing.PhaseProvision,
		timing.PhaseImagePull,
		timing.PhaseContainerCreate,
		timing.PhaseContainerStartup,
		timing.PhaseContainerRun,
		timing.PhaseLogDrain,
		timing.PhaseContainerRemove,
		timing.PhaseTeardown,
	} {
		require.Truef(t, rec.has(p), "phase %s was not recorded", p)
	}
}

// TestScheduleJobFailsOnNonZeroExit asserts a failing step aborts the job.
func TestScheduleJobFailsOnNonZeroExit(t *testing.T) {
	publisher := &capturingPublisher{}
	scheduler := newTestScheduler(t, publisher)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	job := payload.QueuePayload{
		ID:   uuid.New(),
		Name: "failing-job",
		Steps: []payload.Step{
			{ID: 1, Image: testImage, Script: "echo boom >&2; exit 3"},
			{ID: 2, Image: testImage, Script: "echo should-not-run"},
		},
	}

	err := scheduler.ScheduleJob(ctx, job)
	require.Error(t, err)
	require.Contains(t, err.Error(), "container exited with status 3")

	// The second step must not have run, and stderr from the first must still
	// have been captured and published.
	joined := strings.Join(publisher.messages(), "\n")
	require.Contains(t, joined, "boom")
	require.NotContains(t, joined, "should-not-run")
}

// TestScheduleJobTimesOut asserts the whole-job timeout kills a long-running
// step, fails the job promptly, and leaves no container running (so the shared
// volume can be cleaned up).
func TestScheduleJobTimesOut(t *testing.T) {
	publisher := &capturingPublisher{}
	scheduler := newTestScheduler(t, publisher)

	rec := &recordingTracer{}
	timing.SetTracer(rec)
	t.Cleanup(func() { timing.SetTracer(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	job := payload.QueuePayload{
		ID:             uuid.New(),
		Name:           "timeout-job",
		TimeoutSeconds: 2,
		Steps: []payload.Step{
			{ID: 1, Image: testImage, Script: "sleep 120"},
		},
	}

	start := time.Now()
	err := scheduler.ScheduleJob(ctx, job)
	elapsed := time.Since(start)

	require.Error(t, err)
	// The job must abort near the 2s deadline, not run the full 120s sleep.
	require.Less(t, elapsed, 60*time.Second, "job did not abort at the timeout")

	// The cancellation cleanup path must still record container_remove so a
	// timed-out job's timing breakdown is complete.
	require.True(t, rec.has(timing.PhaseContainerRemove), "container_remove not recorded on the timeout path")

	// No container for this job may still be running: a cancelled ContainerWait
	// does not stop the container, so the executor must force-remove it.
	list, listErr := scheduler.cli.ContainerList(ctx, client.ContainerListOptions{
		All:     true,
		Filters: client.Filters{}.Add("label", "job_id="+job.ID.String()),
	})
	require.NoError(t, listErr)
	require.Empty(t, list.Items, "container was leaked after timeout")
}

// TestScheduleJobNetworkNone asserts that network mode "none" fully isolates the
// step container: only the loopback interface is present.
func TestScheduleJobNetworkNone(t *testing.T) {
	publisher := &capturingPublisher{}
	scheduler := newTestScheduler(t, publisher)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// With network "none" the only network interface is "lo"; any other interface
	// (e.g. eth0 on the default bridge) makes this fail.
	job := payload.QueuePayload{
		ID:   uuid.New(),
		Name: "network-none-job",
		Steps: []payload.Step{
			{ID: 1, Image: testImage, Network: "none", Script: `test "$(ls /sys/class/net)" = "lo"`},
		},
	}

	require.NoError(t, scheduler.ScheduleJob(ctx, job))
}

// TestPullImagesReportsErrorForMissingImage asserts that errors the daemon
// reports in-band on the pull stream surface as errors rather than being
// silently drained.
func TestPullImagesReportsErrorForMissingImage(t *testing.T) {
	scheduler := newTestScheduler(t, &capturingPublisher{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	err := pullImages(ctx, scheduler.cli, "hades-scheduler/hades-no-such-image:definitely-missing")
	require.Error(t, err)
}
