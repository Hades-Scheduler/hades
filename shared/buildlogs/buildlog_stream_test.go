package buildlogs

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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

// startJetStream starts a JetStream-enabled NATS container for the watcher tests.
func startJetStream(t *testing.T) *nats.Conn {
	t.Helper()
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.11.4",
			ExposedPorts: []string{"4222/tcp", "8222/tcp"},
			Cmd:          []string{"-js", "-m", "8222"},
			WaitingFor:   wait.ForHTTP("/healthz").WithPort("8222/tcp"),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)
	nc, err := nats.Connect("nats://" + endpoint)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

// collector gathers handler calls from a watcher goroutine.
type collector struct {
	mu   sync.Mutex
	logs []Log
}

func (c *collector) add(l Log) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logs = append(c.logs, l)
}

func (c *collector) entries() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, l := range c.logs {
		n += len(l.Logs)
	}
	return n
}

func TestWatchJobLogs_NATS(t *testing.T) {
	nc := startJetStream(t)
	producer, err := NewHadesLogProducer(nc)
	require.NoError(t, err)
	consumer, err := NewHadesLogConsumer(nc)
	require.NoError(t, err)
	ctx := context.Background()

	publish := func(jobID string, n int) {
		for i := range n {
			require.NoError(t, producer.PublishJobLog(ctx, Log{
				JobID: jobID, ContainerID: "step-1",
				Logs: []LogEntry{entry(fmt.Sprintf("line %d", i))},
			}))
		}
	}

	// A drain requested before the watcher read anything must still deliver the
	// whole backlog, including more messages than one client buffer holds, and
	// must skip a malformed message instead of stalling on it.
	t.Run("drain delivers full backlog and skips malformed", func(t *testing.T) {
		const jobID = "drain-job"
		publish(jobID, defaultPullMaxMessages+50)
		_, err := producer.js.Publish(ctx, fmt.Sprintf(NatsLogSubject, jobID), []byte("{not json"))
		require.NoError(t, err)
		publish(jobID, 3)

		drain := make(chan struct{})
		close(drain)
		var got collector
		start := time.Now()
		require.NoError(t, consumer.WatchJobLogs(ctx, jobID, drain, got.add))
		assert.Equal(t, defaultPullMaxMessages+53, got.entries())
		assert.Less(t, time.Since(start), 10*time.Second)
	})

	t.Run("drain without any logs returns promptly", func(t *testing.T) {
		drain := make(chan struct{})
		close(drain)
		start := time.Now()
		require.NoError(t, consumer.WatchJobLogs(ctx, "no-logs-job", drain, func(Log) {}))
		assert.Less(t, time.Since(start), 2*time.Second)
	})

	// Live logs reach the handler without a drain, via the batch timer, and a
	// later drain picks up what was published after the watcher started.
	t.Run("live tail then drain", func(t *testing.T) {
		const jobID = "live-job"
		drain := make(chan struct{})
		var got collector
		done := make(chan error, 1)
		go func() { done <- consumer.WatchJobLogs(ctx, jobID, drain, got.add) }()

		publish(jobID, 5)
		require.Eventually(t, func() bool { return got.entries() == 5 },
			5*time.Second, 20*time.Millisecond, "live logs must be flushed by the batch timer")

		publish(jobID, 7)
		close(drain)
		require.NoError(t, <-done)
		assert.Equal(t, 12, got.entries())
	})

	t.Run("cancel is a hard stop", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- consumer.WatchJobLogs(cctx, "cancel-job", make(chan struct{}), func(Log) {}) }()
		time.Sleep(100 * time.Millisecond)
		cancel()
		select {
		case err := <-done:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("watcher did not stop on cancel")
		}
	})
}

// seqMsg is a jetstream.Msg carrying only a stream sequence.
type seqMsg struct {
	jetstream.Msg
	seq uint64
}

func (m seqMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.seq}}, nil
}

// scriptedIter yields its messages, then either reports itself closed or blocks
// until ctx ends.
type scriptedIter struct {
	jetstream.MessagesContext
	ctx      context.Context
	msgs     []uint64
	closeErr bool
}

func (it *scriptedIter) Next(...jetstream.NextOpt) (jetstream.Msg, error) {
	if len(it.msgs) > 0 {
		seq := it.msgs[0]
		it.msgs = it.msgs[1:]
		return seqMsg{seq: seq}, nil
	}
	if it.closeErr {
		return nil, jetstream.ErrMsgIteratorClosed
	}
	<-it.ctx.Done()
	return nil, it.ctx.Err()
}

func (it *scriptedIter) Stop() {}

// When the ordered consumer gives up and closes the iterator, the reader must
// reopen right after the last forwarded message instead of going silent, which
// would stall a drain until its timeout and forward partial logs.
func TestReadMessages_ReopensAfterUnexpectedClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := &scriptedIter{ctx: ctx, msgs: []uint64{1, 2, 3}, closeErr: true}
	var reopenedAt []uint64
	open := func(_ context.Context, startSeq uint64) (jetstream.MessagesContext, error) {
		reopenedAt = append(reopenedAt, startSeq)
		return &scriptedIter{ctx: ctx, msgs: []uint64{4, 5}}, nil
	}

	msgs := make(chan streamMsg)
	done := make(chan struct{})
	go readMessages(ctx, first, open, "job", msgs, done)

	var got []uint64
	for range 5 {
		select {
		case m := <-msgs:
			got = append(got, m.seq)
		case <-time.After(5 * time.Second):
			t.Fatalf("reader stalled after %v", got)
		}
	}
	cancel()
	<-done

	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, got)
	assert.Equal(t, []uint64{4}, reopenedAt, "reopen must resume after the last forwarded sequence")
}
