package buildlogs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// NatsLogSubject is the NATS subject pattern for job logs
	NatsLogSubject = "hades.logs.%s"
	// StreamName is the JetStream stream name for job logs
	StreamName = "HADES_JOB_LOGS"
	// Default batch sizes and timeouts
	defaultBatchSize = 100
	// defaultBatchTimeout bounds how long the consumer buffers streamed entries
	// before flushing them to the aggregator. Kept short so the snapshot API
	// reflects live logs promptly; the dashboard SSE path bypasses this timer.
	defaultBatchTimeout = 1 * time.Second
	// defaultPullMaxMessages is how many messages the ordered consumer keeps
	// buffered client-side. A deep buffer lets a drain after a chatty job read
	// its backlog in a few large pulls rather than many small ones.
	defaultPullMaxMessages = 500
	// defaultIteratorRetry is the backoff after an iterator error the ordered
	// consumer could not recover from on its own (e.g. a disconnect).
	defaultIteratorRetry = 500 * time.Millisecond
	// consumerInactiveThreshold is how long the server keeps a per-job consumer
	// nobody is pulling from. The library default of 5 minutes would leave every
	// finished job's consumer around that long.
	consumerInactiveThreshold = 30 * time.Second
	// consumerPullExpiry must stay well below consumerInactiveThreshold, see
	// openIterator.
	consumerPullExpiry = 10 * time.Second
	// defaultDrainQuietPeriod ends a drain whose target sequence is unknown once
	// no message has arrived for this long.
	defaultDrainQuietPeriod = 2 * time.Second
	// defaultDrainTimeout bounds the graceful drain so a stuck consumer can
	// never make WatchJobLogs hang; on timeout we flush what we have and return.
	defaultDrainTimeout = 30 * time.Second
	// JobNamePrefix is the prefix for job names, used in K8s mode
	JobNamePrefix = "buildjob-%s"

	// streamMaxMsgs bounds the total number of log messages retained across all
	// jobs in the HADES_JOB_LOGS stream. Live streaming turns one message per
	// container into many small messages, so this is far above the original
	// per-completed-container cap to avoid evicting other jobs' logs.
	streamMaxMsgs = 200000
	// streamMaxMsgsPerSubject bounds retained messages per job (subject
	// hades.logs.<jobID>), so a single chatty job cannot starve others.
	streamMaxMsgsPerSubject = 5000
)

var (
	// ErrNilConnection is returned when NATS connection is nil
	ErrNilConnection = errors.New("nil NATS connection")
	// ErrNilJetStream is returned when JetStream context is nil
	ErrNilJetStream = errors.New("nil JetStream context")
	// ErrInvalidJobID is returned when job ID is empty or invalid
	ErrInvalidJobID = errors.New("invalid job ID")
)

// LogPublisher defines the interface for publishing logs
type LogPublisher interface {
	PublishJobLog(ctx context.Context, buildJobLog Log) error
}

// LogConsumer defines the interface for consuming logs
type LogConsumer interface {
	WatchJobLogs(ctx context.Context, jobID string, drain <-chan struct{}, handler func(Log)) error
}

// HadesLogProducer handles publishing logs
type HadesLogProducer struct {
	natsConnection *nats.Conn
	js             jetstream.JetStream
}

// HadesLogConsumer handles consuming logs
type HadesLogConsumer struct {
	natsConnection *nats.Conn
	js             jetstream.JetStream

	streamMu sync.Mutex
	stream   jetstream.Stream
}

// NewHadesLogProducer creates a new log producer with JetStream stream setup.
// It creates or updates the HADES_JOB_LOGS stream with file storage and 24-hour retention.
// Returns an error if the NATS connection is nil or stream creation fails.
func NewHadesLogProducer(nc *nats.Conn) (*HadesLogProducer, error) {
	if nc == nil {
		return nil, ErrNilConnection
	}

	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("creating JetStream context: %w", err)
	}

	streamConfig := jetstream.StreamConfig{
		Name:              StreamName,
		Subjects:          []string{fmt.Sprintf(NatsLogSubject, "*")},
		Storage:           jetstream.FileStorage,
		Retention:         jetstream.LimitsPolicy,
		Duplicates:        1 * time.Minute,
		MaxMsgs:           streamMaxMsgs,
		MaxMsgsPerSubject: streamMaxMsgsPerSubject,
		MaxAge:            24 * time.Hour,
	}

	stream, err := js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		return nil, fmt.Errorf("creating JetStream stream: %w", err)
	}

	slog.Info("JetStream stream ready",
		"stream", stream.CachedInfo().Config.Name,
		"subjects", stream.CachedInfo().Config.Subjects)

	return &HadesLogProducer{
		natsConnection: nc,
		js:             js,
	}, nil
}

// NewHadesLogConsumer creates a new log consumer for reading logs from JetStream.
// Returns an error if the NATS connection is nil or JetStream context creation fails.
func NewHadesLogConsumer(nc *nats.Conn) (*HadesLogConsumer, error) {
	if nc == nil {
		return nil, ErrNilConnection
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("creating JetStream context: %w", err)
	}

	return &HadesLogConsumer{
		natsConnection: nc,
		js:             js,
	}, nil
}

// PublishJobLog publishes a log entry to JetStream for the specified job.
// The log is published to the subject "hades.logs.{jobID}".
// Returns an error if JetStream is nil, the log is invalid, or publishing fails.
func (hlp *HadesLogProducer) PublishJobLog(ctx context.Context, buildJobLog Log) error {
	if hlp.js == nil {
		return ErrNilJetStream
	}

	if buildJobLog.JobID == "" {
		return ErrInvalidJobID
	}

	subject := fmt.Sprintf(NatsLogSubject, buildJobLog.JobID)
	data, err := json.Marshal(buildJobLog)
	if err != nil {
		return fmt.Errorf("marshaling log for job %s: %w", buildJobLog.JobID, err)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	_, err = hlp.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("publishing log to subject %s: %w", subject, err)
	}

	slog.Debug("Published log",
		"job_id", buildJobLog.JobID,
		"container_id", buildJobLog.ContainerID,
		"entries", len(buildJobLog.Logs))

	return nil
}

// WatchJobLogs subscribes to logs for a specific job and calls the handler for each batch.
// It reads through an ephemeral ordered consumer and batches log entries per container.
//
// There are two ways to stop watching:
//   - Closing drain requests a graceful stop: the watcher keeps reading until it has seen
//     the last message stored for the job's subject at the moment the drain started (or a
//     bounded timeout elapses), so every already-published batch is delivered to the
//     handler before returning. Job logs are published to JetStream before the job's
//     terminal status is announced, so a graceful drain guarantees the in-memory
//     aggregation is complete.
//   - Cancelling ctx is a hard stop for service shutdown: remaining received logs are
//     flushed and the method returns immediately without waiting to catch up.
//
// Returns an error if the job ID is invalid or consumer creation fails.
func (hlc *HadesLogConsumer) WatchJobLogs(ctx context.Context, jobID string, drain <-chan struct{}, handler func(Log)) error {
	if jobID == "" {
		return ErrInvalidJobID
	}

	if hlc.js == nil {
		return ErrNilJetStream
	}

	subject := fmt.Sprintf(NatsLogSubject, jobID)
	open := func(ctx context.Context, startSeq uint64) (jetstream.MessagesContext, error) {
		return hlc.openIterator(ctx, subject, startSeq)
	}

	// Open the first iterator here so a consumer that cannot be created at all is
	// reported to the caller instead of being retried in the background.
	iter, err := open(ctx, 0)
	if err != nil {
		return fmt.Errorf("creating consumer for job %s: %w", jobID, err)
	}

	slog.Info("Started watching job logs", "job_id", jobID, "subject", subject)

	msgs := make(chan streamMsg)
	srcCtx, stop := context.WithCancel(context.Background())
	readerDone := make(chan struct{})
	go readMessages(srcCtx, iter, open, jobID, msgs, readerDone)
	defer func() {
		stop()
		<-readerDone
	}()

	return hlc.processBatchedLogs(ctx, subject, jobID, msgs, drain, handler)
}

// openIterator creates an ordered consumer on subject and starts iterating it,
// from startSeq when non-zero and from the beginning otherwise.
func (hlc *HadesLogConsumer) openIterator(ctx context.Context, subject string, startSeq uint64) (jetstream.MessagesContext, error) {
	// An ordered consumer is ephemeral, unacked and server-side flow controlled:
	// the server pushes new messages into a client buffer instead of the watcher
	// polling, and there is no durable state to create or delete per job. The
	// server reaps it after InactiveThreshold, so returning does not wait on a
	// DeleteConsumer round-trip before the log callback can go out.
	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{subject},
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		InactiveThreshold: consumerInactiveThreshold,
	}
	if startSeq > 0 {
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = startSeq
	}
	consumer, err := hlc.js.OrderedConsumer(ctx, StreamName, cfg)
	if err != nil {
		return nil, err
	}
	// Pull requests expire well inside InactiveThreshold, so a watcher that is
	// merely idle keeps renewing them and its consumer is never reaped under it.
	return consumer.Messages(
		jetstream.PullMaxMessages(defaultPullMaxMessages),
		jetstream.PullExpiry(consumerPullExpiry),
	)
}

// iteratorOpener opens a message iterator starting at startSeq (0 = beginning).
type iteratorOpener func(ctx context.Context, startSeq uint64) (jetstream.MessagesContext, error)

// streamMsg is a log message with its stream sequence, parsed once by
// readMessages (which needs it to reopen) rather than again by the consumer.
type streamMsg struct {
	msg jetstream.Msg
	seq uint64
}

// readMessages pumps iterators into msgs until ctx is cancelled. It exists so the
// batching loop can select on new messages alongside its timer, drain and
// cancellation signals; Next itself only blocks.
//
// The ordered consumer recreates itself on heartbeat and sequence gaps, but it
// closes the iterator for good when that recreation fails (e.g. the connection
// was closed meanwhile). Rather than leaving the batching loop waiting for
// messages that never come, which would turn a drain into a full timeout and a
// partial log, the reader reopens from just after the last forwarded message.
func readMessages(ctx context.Context, iter jetstream.MessagesContext, open iteratorOpener, jobID string, msgs chan<- streamMsg, done chan<- struct{}) {
	defer close(done)
	var lastSeq uint64
	backoff := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(defaultIteratorRetry):
			return true
		}
	}

	for {
		msg, err := iter.Next(jetstream.NextContext(ctx))
		switch {
		case ctx.Err() != nil:
			iter.Stop()
			return
		case errors.Is(err, jetstream.ErrMsgIteratorClosed):
			slog.Warn("Log iterator closed unexpectedly; reopening", "job_id", jobID, "after_seq", lastSeq)
			iter.Stop()
			for {
				if !backoff() {
					return
				}
				next, err := open(ctx, lastSeq+1)
				if err == nil {
					iter = next
					break
				}
				slog.Warn("Failed to reopen log iterator", "job_id", jobID, "error", err)
			}
			continue
		case err != nil:
			// Transient (e.g. a timeout while disconnected); back off instead of
			// spinning.
			slog.Debug("Log iterator error", "job_id", jobID, "error", err)
			if !backoff() {
				iter.Stop()
				return
			}
			continue
		}

		if meta, err := msg.Metadata(); err == nil {
			lastSeq = meta.Sequence.Stream
		}
		select {
		case msgs <- streamMsg{msg: msg, seq: lastSeq}:
		case <-ctx.Done():
			iter.Stop()
			return
		}
	}
}

// containerLogBatcher accumulates log entries per container so that the
// per-step grouping (and the ContainerID) is preserved when logs are handed to
// downstream consumers. Flattening entries across containers would lose the
// step boundaries that consumers such as the Artemis adapter rely on to locate
// a specific step's logs.
type containerLogBatcher struct {
	jobID   string
	order   []string // container IDs in first-seen order
	batches map[string][]LogEntry
	total   int // total buffered entries across all containers
}

func newContainerLogBatcher(jobID string) *containerLogBatcher {
	return &containerLogBatcher{
		jobID:   jobID,
		batches: make(map[string][]LogEntry),
	}
}

// add buffers the entries of an incoming Log under its ContainerID, preserving
// first-seen container order. A Log with no entries still registers the
// container's slot: streaming producers emit a zero-entry Log when a container
// starts so that a step producing no output keeps its position, which the
// Artemis adapter relies on to locate a step's logs by index.
func (b *containerLogBatcher) add(l Log) {
	if l.ContainerID == "" {
		return
	}
	if _, ok := b.batches[l.ContainerID]; !ok {
		b.order = append(b.order, l.ContainerID)
		b.batches[l.ContainerID] = nil
	}
	b.batches[l.ContainerID] = append(b.batches[l.ContainerID], l.Logs...)
	b.total += len(l.Logs)
}

// size returns the total number of buffered entries across all containers.
func (b *containerLogBatcher) size() int { return b.total }

// drain returns one Log per registered container, in first-seen order, each
// carrying its ContainerID (possibly with no entries for a slot-only
// registration), and resets the buffer.
func (b *containerLogBatcher) drain() []Log {
	if len(b.order) == 0 {
		return nil
	}
	out := make([]Log, 0, len(b.order))
	for _, cid := range b.order {
		out = append(out, Log{JobID: b.jobID, ContainerID: cid, Logs: b.batches[cid]})
	}
	b.order = nil
	b.batches = make(map[string][]LogEntry)
	b.total = 0
	return out
}

// processBatchedLogs handles the batching and processing of log messages from the consumer.
// It batches log entries per container for efficiency (preserving the ContainerID and
// per-step grouping) and calls the handler once per container on each flush.
//
// It returns when ctx is cancelled (hard stop: flush and return ctx.Err()) or when drain
// is closed (graceful stop: read up to the subject's last stored message, then flush and
// return nil).
func (hlc *HadesLogConsumer) processBatchedLogs(ctx context.Context, subject, jobID string, msgs <-chan streamMsg, drain <-chan struct{}, handler func(Log)) error {
	batcher := newContainerLogBatcher(jobID)
	batchTimer := time.NewTimer(defaultBatchTimeout)
	defer batchTimer.Stop()

	flushBatch := func() {
		for _, l := range batcher.drain() {
			handler(l)
		}
		batchTimer.Reset(defaultBatchTimeout)
	}

	// lastSeq is the stream sequence of the newest message processed so far. The
	// drain compares it against the subject's last stored sequence.
	var lastSeq uint64
	process := func(m streamMsg) {
		lastSeq = m.seq

		var log Log
		if err := json.Unmarshal(m.msg.Data(), &log); err != nil {
			// Nothing is acked on an ordered consumer, so skipping is final: a
			// malformed message cannot be redelivered in a loop.
			slog.Warn("Skipping malformed log message", "job_id", jobID, "error", err)
			return
		}
		batcher.add(log)
		if batcher.size() >= defaultBatchSize {
			flushBatch()
		}
	}

	for {
		// Hard stop takes priority: on shutdown flush what we have and return.
		select {
		case <-ctx.Done():
			flushBatch()
			return ctx.Err()
		default:
		}

		select {
		case <-ctx.Done():
			flushBatch()
			return ctx.Err()
		case <-drain:
			return hlc.drainToCaughtUp(ctx, subject, jobID, msgs, &lastSeq, process, flushBatch)
		case <-batchTimer.C:
			flushBatch()
		case msg := <-msgs:
			process(msg)
		}
	}
}

// drainToCaughtUp reads until the watcher has processed the last message stored for
// the job's subject when the drain started, then flushes and returns. Because job logs
// are published to JetStream before the terminal status is announced, everything for
// the job is already in the stream by then, so reaching that sequence means every
// batch has been delivered and aggregated. The target is taken once instead of polling
// consumer info: messages already buffered client-side count as delivered there, which
// would end the drain before they were processed. The drain is bounded by
// defaultDrainTimeout so it can never hang; on timeout it flushes whatever it has.
func (hlc *HadesLogConsumer) drainToCaughtUp(ctx context.Context, subject, jobID string, msgs <-chan streamMsg, lastSeq *uint64, process func(streamMsg), flushBatch func()) error {
	slog.Info("Draining remaining job logs before stopping", "job_id", jobID)

	drainCtx, cancel := context.WithTimeout(ctx, defaultDrainTimeout)
	defer cancel()

	target, err := hlc.lastSubjectSeq(drainCtx, subject)
	switch {
	case errors.Is(err, jetstream.ErrMsgNotFound):
		// Nothing was ever stored for this job (or it all aged out).
		flushBatch()
		return nil
	case err != nil:
		// Without a target we cannot know when we are caught up; keep reading
		// until the stream goes quiet for a moment instead of dropping logs.
		slog.Warn("Failed to read last log sequence during drain",
			"job_id", jobID, "error", err)
		return hlc.drainUntilQuiet(drainCtx, jobID, msgs, process, flushBatch)
	}

	for *lastSeq < target {
		select {
		case msg := <-msgs:
			process(msg)
		case <-drainCtx.Done():
			flushBatch()
			slog.Warn("Drain timed out; flushing partial logs",
				"job_id", jobID, "error", drainCtx.Err())
			return nil
		}
	}

	flushBatch()
	slog.Info("Drained job logs to caught-up", "job_id", jobID)
	return nil
}

// drainUntilQuiet is the fallback drain when the target sequence is unknown: it reads
// until no message arrives for defaultDrainQuietPeriod or drainCtx ends.
func (hlc *HadesLogConsumer) drainUntilQuiet(drainCtx context.Context, jobID string, msgs <-chan streamMsg, process func(streamMsg), flushBatch func()) error {
	quiet := time.NewTimer(defaultDrainQuietPeriod)
	defer quiet.Stop()
	for {
		select {
		case msg := <-msgs:
			process(msg)
			quiet.Reset(defaultDrainQuietPeriod)
		case <-quiet.C:
			flushBatch()
			return nil
		case <-drainCtx.Done():
			flushBatch()
			slog.Warn("Drain timed out; flushing partial logs",
				"job_id", jobID, "error", drainCtx.Err())
			return nil
		}
	}
}

// lastSubjectSeq returns the stream sequence of the newest message stored on subject.
func (hlc *HadesLogConsumer) lastSubjectSeq(ctx context.Context, subject string) (uint64, error) {
	stream, err := hlc.logStream(ctx)
	if err != nil {
		return 0, err
	}
	msg, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return 0, err
	}
	return msg.Sequence, nil
}

// logStream returns a cached handle on the log stream, binding it on first use. The
// handle is only used for direct gets, which do not depend on the cached stream info.
func (hlc *HadesLogConsumer) logStream(ctx context.Context) (jetstream.Stream, error) {
	hlc.streamMu.Lock()
	defer hlc.streamMu.Unlock()
	if hlc.stream != nil {
		return hlc.stream, nil
	}
	stream, err := hlc.js.Stream(ctx, StreamName)
	if err != nil {
		return nil, fmt.Errorf("binding stream %s: %w", StreamName, err)
	}
	hlc.stream = stream
	return stream, nil
}
