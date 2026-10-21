package buildlogs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func entry(msg string) LogEntry {
	return LogEntry{Timestamp: time.Unix(0, 0), Message: msg, OutputStream: "stdout"}
}

// The Artemis adapter locates a step's logs positionally (e.g. logs[1] for the
// execution step), so the batcher must preserve one Log per container in
// first-seen order and keep each ContainerID.
func TestContainerLogBatcher_GroupsPerContainerInOrder(t *testing.T) {
	b := newContainerLogBatcher("job-1")
	b.add(Log{JobID: "job-1", ContainerID: "step-1", Logs: []LogEntry{entry("clone a"), entry("clone b")}})
	b.add(Log{JobID: "job-1", ContainerID: "step-2", Logs: []LogEntry{entry("build")}})

	assert.Equal(t, 3, b.size())

	out := b.drain()
	require.Len(t, out, 2, "one Log per container")

	assert.Equal(t, "step-1", out[0].ContainerID)
	assert.Equal(t, "job-1", out[0].JobID)
	require.Len(t, out[0].Logs, 2)
	assert.Equal(t, "clone a", out[0].Logs[0].Message)

	assert.Equal(t, "step-2", out[1].ContainerID)
	require.Len(t, out[1].Logs, 1)
	assert.Equal(t, "build", out[1].Logs[0].Message)
}

func TestContainerLogBatcher_MergesSameContainer(t *testing.T) {
	b := newContainerLogBatcher("job-1")
	b.add(Log{ContainerID: "step-2", Logs: []LogEntry{entry("part 1")}})
	b.add(Log{ContainerID: "step-2", Logs: []LogEntry{entry("part 2")}})

	out := b.drain()
	require.Len(t, out, 1, "entries for the same container merge into one Log")
	require.Len(t, out[0].Logs, 2)
	assert.Equal(t, "part 1", out[0].Logs[0].Message)
	assert.Equal(t, "part 2", out[0].Logs[1].Message)
}

func TestContainerLogBatcher_RegistersEmptySlotAndResetsOnDrain(t *testing.T) {
	b := newContainerLogBatcher("job-1")

	// A Log with no ContainerID is ignored entirely.
	b.add(Log{Logs: nil})
	assert.Equal(t, 0, b.size())
	assert.Nil(t, b.drain())

	// A zero-entry Log registers the container's slot (size counts entries, so
	// it stays 0) but drains as one empty Log so the position is preserved.
	b.add(Log{ContainerID: "step-1", Logs: nil})
	assert.Equal(t, 0, b.size())
	out := b.drain()
	require.Len(t, out, 1)
	assert.Equal(t, "step-1", out[0].ContainerID)
	assert.Empty(t, out[0].Logs)

	// Buffer was reset by drain.
	assert.Nil(t, b.drain())
	assert.Equal(t, 0, b.size())
}

// A zero-output step must keep its slot so later steps do not shift into its
// index. Streaming registers step-1 (empty) before step-2 produces output, so
// the execute step stays at index 1 for the Artemis adapter.
func TestContainerLogBatcher_EmptySlotPreservesOrder(t *testing.T) {
	b := newContainerLogBatcher("job-1")
	b.add(Log{ContainerID: "step-1", Logs: nil}) // clone produced nothing
	b.add(Log{ContainerID: "step-2", Logs: []LogEntry{entry("build")}})

	out := b.drain()
	require.Len(t, out, 2)
	assert.Equal(t, "step-1", out[0].ContainerID)
	assert.Empty(t, out[0].Logs)
	assert.Equal(t, "step-2", out[1].ContainerID)
	require.Len(t, out[1].Logs, 1)
}

func TestWaitIdle_WakesOnDrain(t *testing.T) {
	stop := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(stop)
	}()

	start := time.Now()
	waitIdle(context.Background(), stop, 5*time.Second)
	assert.Less(t, time.Since(start), time.Second, "drain must interrupt the idle backoff")
}

func TestWaitIdle_WakesOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	waitIdle(ctx, nil, 5*time.Second)
	assert.Less(t, time.Since(start), time.Second, "cancel must interrupt the idle backoff")
}

func TestWaitIdle_SleepsFullDurationWithoutSignal(t *testing.T) {
	start := time.Now()
	waitIdle(context.Background(), nil, 50*time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}
