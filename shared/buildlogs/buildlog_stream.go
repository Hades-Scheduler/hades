package buildlogs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	defaultBatchTimeout  = 1 * time.Second
	defaultFetchSize     = 10
	defaultFetchWaitTime = 100 * time.Millisecond
	defaultShutdownTime  = 5 * time.Second
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
// It creates a job-specific durable consumer and automatically batches log entries for efficiency.
//
// There are two ways to stop watching:
//   - Closing drain requests a graceful stop: the consumer keeps fetching until it is
//     caught up to the stream (NumPending == 0) or a bounded timeout elapses, so every
//     already-published batch is delivered to the handler before returning. Job logs are
//     published to JetStream before the job's terminal status is announced, so a graceful
//     drain guarantees the in-memory aggregation is complete.
//   - Cancelling ctx is a hard stop for service shutdown: remaining fetched logs are
//     flushed and the method returns immediately without waiting to catch up.
//
// The consumer is automatically cleaned up when the method returns.
// Returns an error if the job ID is invalid or consumer creation fails.
func (hlc *HadesLogConsumer) WatchJobLogs(ctx context.Context, jobID string, drain <-chan struct{}, handler func(Log)) error {
	if jobID == "" {
		return ErrInvalidJobID
	}

	if hlc.js == nil {
		return ErrNilJetStream
	}

	subject := fmt.Sprintf(NatsLogSubject, jobID)
	consumerName := fmt.Sprintf("job-watcher-%s", jobID)

	consumerConfig := jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}

	consumer, err := hlc.js.CreateOrUpdateConsumer(ctx, StreamName, consumerConfig)
	if err != nil {
		return fmt.Errorf("creating consumer for job %s: %w", jobID, err)
	}

	// Cleanup consumer when done
	defer func() {
		deleteCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTime)
		defer cancel()

		if err := hlc.js.DeleteConsumer(deleteCtx, StreamName, consumerName); err != nil {
			slog.Warn("Failed to delete job consumer",
				"job_id", jobID,
				"consumer", consumerName,
				"error", err)
		} else {
			slog.Debug("Deleted job consumer",
				"job_id", jobID,
				"consumer", consumerName)
		}
	}()

	slog.Info("Started watching job logs",
		"job_id", jobID,
		"subject", subject,
		"consumer", consumerName)

	return hlc.processBatchedLogs(ctx, consumer, jobID, drain, handler)
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
// is closed (graceful stop: drain the consumer to caught-up, then flush and return nil).
func (hlc *HadesLogConsumer) processBatchedLogs(ctx context.Context, consumer jetstream.Consumer, jobID string, drain <-chan struct{}, handler func(Log)) error {
	batcher := newContainerLogBatcher(jobID)
	batchTimer := time.NewTimer(defaultBatchTimeout)
	defer batchTimer.Stop()

	flushBatch := func() {
		for _, l := range batcher.drain() {
			handler(l)
		}
		batchTimer.Reset(defaultBatchTimeout)
	}

	// fetchOnce pulls up to defaultFetchSize messages, batching and acking each,
	// flushing when the batch grows large. It returns true if any message was
	// received. It is shared by the steady-state loop and the drain loop so both
	// consume from the same durable position, guaranteeing no message is read or
	// aggregated twice. After an empty fetch it backs off until stop closes. The
	// drain loop passes nil because drain is already closed there and the backoff
	// would turn into a busy loop.
	fetchOnce := func(stop <-chan struct{}) bool {
		batch, err := consumer.FetchNoWait(defaultFetchSize)
		if err != nil {
			waitIdle(ctx, stop, defaultFetchWaitTime)
			return false
		}

		hasMessages := false
		for msg := range batch.Messages() {
			hasMessages = true

			var log Log
			if err := json.Unmarshal(msg.Data(), &log); err != nil {
				slog.Warn("Failed to unmarshal log message",
					"job_id", jobID,
					"error", err)
				if ackErr := msg.Nak(); ackErr != nil {
					slog.Warn("Failed to NAK message", "error", ackErr)
				}
				continue
			}

			batcher.add(log)

			if ackErr := msg.Ack(); ackErr != nil {
				slog.Warn("Failed to ACK message", "error", ackErr)
			}

			// Flush batch if it gets too large
			if batcher.size() >= defaultBatchSize {
				flushBatch()
			}
		}

		if !hasMessages {
			waitIdle(ctx, stop, defaultFetchWaitTime)
		}
		return hasMessages
	}

	for {
		// Hard stop takes priority: on shutdown flush what we have and return.
		select {
		case <-ctx.Done():
			flushBatch()
			return ctx.Err()
		default:
		}

		// Graceful stop: drain the consumer to caught-up before returning so the
		// in-memory aggregation is complete. Checked before the fetch below so a
		// closed drain channel is taken promptly instead of racing the default case.
		select {
		case <-drain:
			return hlc.drainToCaughtUp(ctx, consumer, jobID, fetchOnce, flushBatch)
		default:
		}

		select {
		case <-batchTimer.C:
			flushBatch()
		default:
			fetchOnce(drain)
		}
	}
}

// drainToCaughtUp keeps fetching from the durable consumer until it is caught up to
// the stream (ConsumerInfo.NumPending == 0), then flushes and returns. Because job
// logs are published to JetStream before the terminal status is announced, everything
// for the job is already in the stream by the time a drain is requested, so reaching
// NumPending == 0 means every batch has been delivered and aggregated. The drain is
// bounded by defaultDrainTimeout so it can never hang; on timeout it flushes whatever
// it has and returns. It reuses the same consumer position as the steady-state loop,
// so no message is re-read or duplicated.
func (hlc *HadesLogConsumer) drainToCaughtUp(ctx context.Context, consumer jetstream.Consumer, jobID string, fetchOnce func(stop <-chan struct{}) bool, flushBatch func()) error {
	slog.Info("Draining remaining job logs before stopping", "job_id", jobID)

	drainCtx, cancel := context.WithTimeout(ctx, defaultDrainTimeout)
	defer cancel()

	for {
		info, err := consumer.Info(drainCtx)
		if err != nil {
			slog.Warn("Failed to read consumer info during drain",
				"job_id", jobID, "error", err)
		} else if info.NumPending == 0 {
			flushBatch()
			slog.Info("Drained job logs to caught-up", "job_id", jobID)
			return nil
		}

		if drainCtx.Err() != nil {
			flushBatch()
			slog.Warn("Drain timed out; flushing partial logs",
				"job_id", jobID, "error", drainCtx.Err())
			return nil
		}

		fetchOnce(nil)
	}
}

// waitIdle backs off for d between empty fetches, but wakes early when stop is closed
// or ctx is cancelled so a stop request is not delayed by the remaining backoff.
func waitIdle(ctx context.Context, stop <-chan struct{}, d time.Duration) {
	select {
	case <-time.After(d):
	case <-stop:
	case <-ctx.Done():
	}
}
