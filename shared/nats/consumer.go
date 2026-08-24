package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var _ hades.JobConsumer = (*HadesNATSConsumer)(nil)

const (
	// DefaultAckWait is how long JetStream waits for an ack (or an in-progress
	// signal) before it redelivers a job to another worker. It is deliberately
	// unrelated to how long a job runs: while a job is being processed the
	// worker resets this timer every ackProgressInterval (see startAckProgress),
	// so AckWait only bounds how long an *unresponsive* worker keeps a job
	// hostage. One minute means a crashed or partitioned scheduler releases its
	// in-flight jobs quickly, while still tolerating six consecutive lost
	// heartbeats before a healthy worker's job is handed to someone else.
	DefaultAckWait = 1 * time.Minute

	// DefaultMaxDeliver bounds how often a single job may be delivered. Without
	// it JetStream retries forever, so a job that reliably kills its worker
	// loops for the stream's whole 24h retention. The last delivery is not
	// executed: it is used to report the terminal failure and terminate the
	// message (see processJob), so the default allows two execution attempts.
	DefaultMaxDeliver = 3

	// DefaultConcurrency is how many jobs one scheduler instance processes at
	// once. Named rather than inlined so the envDefault tag below, withDefaults
	// and the documentation all point at one value.
	DefaultConcurrency = 1

	// maxAckProgressInterval caps the in-progress heartbeat so a long AckWait
	// does not lead to a needlessly coarse heartbeat.
	maxAckProgressInterval = 10 * time.Second

	// minAckProgressInterval keeps the heartbeat from becoming a busy loop when
	// a very small AckWait is configured (mainly in tests).
	minAckProgressInterval = 100 * time.Millisecond
)

// ConsumerConfig holds the tunables of the JetStream job consumer.
// Zero values fall back to the defaults above.
//
// The envDefault tags matter beyond convenience. withDefaults() below is
// applied inside NewHadesConsumer, so the consumer always RUNS with the right
// values - but without these tags a config parsed from an environment that sets
// none of them stays at zero, and anything inspecting it (notably the
// scheduler's startup log) reports 0/0s/0 while the consumer actually uses
// 1/1m/3. A configuration log that disagrees with the effective configuration
// is worse than no log, so the defaults are declared here as well.
//
// The tag literals must stay in step with the constants above; the values are
// duplicated because struct tags cannot reference Go constants.
// TestConsumerConfigEnvDefaultsMatchConstants pins them together.
type ConsumerConfig struct {
	// Concurrency is the maximum number of jobs processed simultaneously by a
	// single scheduler instance.
	Concurrency uint `env:"CONCURRENCY" envDefault:"1"`
	// AckWait is the JetStream ack timeout for an in-flight job. See
	// DefaultAckWait: it is a liveness backstop, not a job-duration budget.
	AckWait time.Duration `env:"NATS_ACK_WAIT" envDefault:"1m"`
	// MaxDeliver is the maximum number of times a single job is delivered. See
	// DefaultMaxDeliver.
	MaxDeliver int `env:"NATS_MAX_DELIVER" envDefault:"3"`
}

// withDefaults returns the config with non-positive values replaced by the
// package defaults. Still required despite the envDefault tags above: a
// ConsumerConfig built programmatically (tests, direct struct literals) never
// goes through env.Parse and would otherwise reach JetStream as zeros.
func (c ConsumerConfig) withDefaults() ConsumerConfig {
	if c.Concurrency == 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.AckWait <= 0 {
		c.AckWait = DefaultAckWait
	}
	if c.MaxDeliver <= 0 {
		c.MaxDeliver = DefaultMaxDeliver
	}
	return c
}

// minMaxDeliver is the smallest MaxDeliver that still lets a job run. The last
// allowed delivery is spent reporting the terminal failure instead of executing
// the job (see processJob), so MaxDeliver = 1 would mean the very first delivery
// is already the last one and no job would ever be executed.
const minMaxDeliver = 2

// minAckWait is the smallest AckWait that preserves the heartbeat's safety
// margin. ackProgressInterval targets AckWait/3 so at least three heartbeats
// fall inside every ack window and a couple of delayed or lost ticks still
// cannot trigger a redelivery. Below this bound the minAckProgressInterval
// clamp wins, that margin shrinks towards a single tick, and under 100ms it
// disappears entirely: the first InProgress call would land after AckWait had
// already elapsed and JetStream would redeliver a job that is still running.
const minAckWait = 3 * minAckProgressInterval

// validate rejects a configuration that would silently break job execution.
// It runs after withDefaults, so a zero or negative value has already been
// replaced by its default and only an explicit, unusable one is left.
func (c ConsumerConfig) validate() error {
	if c.AckWait < minAckWait {
		return fmt.Errorf(
			"NATS_ACK_WAIT must be at least %s, got %s: the in-progress heartbeat is clamped to a %s floor, so a shorter AckWait leaves fewer than three heartbeats per ack window and loses the margin that keeps a delayed or dropped tick from redelivering a job that is still running",
			minAckWait, c.AckWait, minAckProgressInterval)
	}
	if c.MaxDeliver < minMaxDeliver {
		return fmt.Errorf(
			"NATS_MAX_DELIVER must be at least %d, got %d: the last allowed delivery reports the terminal failure instead of running the job, so a lower value would never execute any job",
			minMaxDeliver, c.MaxDeliver)
	}
	return nil
}

// ackProgressInterval returns how often an in-flight job signals progress for
// the given AckWait. It stays well below AckWait so several lost heartbeats do
// not trigger a redelivery.
func ackProgressInterval(ackWait time.Duration) time.Duration {
	interval := ackWait / 3
	if interval > maxAckProgressInterval {
		interval = maxAckProgressInterval
	}
	if interval < minAckProgressInterval {
		interval = minAckProgressInterval
	}
	return interval
}

// HadesNATSConsumer manages job consumption from the NATS queue system with priority handling.
type HadesNATSConsumer struct {
	natsConnection *nats.Conn
	concurrency    uint
	ackWait        time.Duration
	maxDeliver     int
	consumers      map[hades.Priority]jetstream.Consumer
	kv             jetstream.KeyValue
}

// NewHadesConsumer creates a new job consumer with priority queue support.
// The config controls how many jobs are processed simultaneously and how
// JetStream redelivers jobs whose worker stops responding.
func NewHadesConsumer(nc *nats.Conn, cfg ConsumerConfig) (*HadesNATSConsumer, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		slog.Error("Invalid consumer configuration", "error", err)
		return nil, err
	}

	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("Failed to create JetStream context", "error", err)
		return nil, err
	}
	consumers := make(map[hades.Priority]jetstream.Consumer, len(hades.Priorities))

	// Create a consumer for each priority (one consumer is shared across all worker nodes)
	for _, priority := range hades.Priorities {
		consumerName := fmt.Sprintf("HADES_JOBS_%s", priority)
		cons, err := js.CreateOrUpdateConsumer(ctx, "HADES_JOBS", jetstream.ConsumerConfig{
			Name:          consumerName,
			Durable:       consumerName,
			AckPolicy:     jetstream.AckExplicitPolicy,
			FilterSubject: prioritySubject(priority),
			// Set both explicitly instead of inheriting the client defaults
			// (AckWait 30s, MaxDeliver unlimited): a 30s ack timeout redelivered
			// every job that ran longer than half a minute, and an unlimited
			// MaxDeliver turned any job that kills its worker into an endless
			// loop.
			AckWait:    cfg.AckWait,
			MaxDeliver: cfg.MaxDeliver,
		})
		if err != nil {
			slog.Error("Failed to create JetStream consumer", "error", err, "priority", priority)
			return nil, err
		}
		consumers[priority] = cons
		slog.Info("Created JetStream consumer", "consumer", consumerName, "priority", prioritySubject(priority),
			"ack_wait", cfg.AckWait, "max_deliver", cfg.MaxDeliver)
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "HADES_JOBS",
	})
	if err != nil {
		slog.Error("Failed to create JetStream KeyValue store", "error", err)
		return nil, err
	}
	return &HadesNATSConsumer{
		natsConnection: nc,
		consumers:      consumers,
		concurrency:    cfg.Concurrency,
		ackWait:        cfg.AckWait,
		maxDeliver:     cfg.MaxDeliver,
		kv:             kv,
	}, nil
}

// DequeueJob continuously processes jobs from the queue in priority order.
// It spawns worker goroutines up to the configured concurrency limit and processes
// jobs using the provided processing function. Workers fetch jobs on-demand to ensure
// fair distribution across multiple consumer instances.
func (hc *HadesNATSConsumer) DequeueJob(ctx context.Context, processing hades.PayloadHandler) {
	var wg sync.WaitGroup

	// Create workers that fetch their own jobs (pull model instead of push)
	for i := uint(0); i < hc.concurrency; i++ {
		wg.Add(1)
		go func(workerID uint) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					slog.Error("Worker panic recovered", "workerID", workerID, "panic", r)
				}
			}()

			// Each worker continuously fetches and processes jobs
			hc.workerLoop(ctx, workerID, processing)
		}(i)
	}

	// Wait for all workers to complete
	wg.Wait()
	slog.Info("All workers finished, dequeue complete")
}

// workerLoop handles the fetch-process loop for a single worker
func (hc *HadesNATSConsumer) workerLoop(ctx context.Context, workerID uint, processing hades.PayloadHandler) {
	// Exponential backoff for when no messages are available

	const resetBackoff = 10 * time.Millisecond
	const maxBackoff = 1 * time.Second
	backoff := resetBackoff

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled, worker stopping", "workerID", workerID)
			return
		default:
		}

		// Only fetch when this worker is ready to process
		msg, priority, found := hc.fetchNextMessage(ctx)

		// If no message was found in any queue, sleep with backoff and retry
		if !found {
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
				continue
			case <-ctx.Done():
				slog.Info("Context cancelled during backoff", "workerID", workerID)
				return
			}
		}

		// Reset backoff on successful fetch
		backoff = resetBackoff

		// Parse and validate the message
		job, err := hc.parseMessage(ctx, msg)
		if err != nil {
			// Error already logged in parseMessage
			continue
		}

		if job.Metadata == nil {
			job.Metadata = map[string]string{}
		}
		job.Metadata[hades.MetadataKeyPriority] = strconv.Itoa(hades.PriorityToInt(priority))
		job.Metadata[hades.MetadataKeyPriorityName] = string(priority)

		// Process the job immediately (we have capacity)
		hc.processJob(workerID, job, msg, priority, processing)
	}
}

// processJob handles individual job processing with error recovery
func (hc *HadesNATSConsumer) processJob(
	workerID uint,
	job payload.QueuePayload,
	msg jetstream.Msg,
	priority hades.Priority,
	processing hades.PayloadHandler,
) {
	// A job that reaches its last allowed delivery has already stalled or
	// crashed a worker on every previous attempt. Running it once more would
	// only burn another AckWait and then be dropped silently by JetStream, so
	// the final delivery is spent reporting the terminal failure instead - the
	// same Failed status the executors publish for any other job failure.
	if numDelivered := deliveryCount(msg, job.ID.String()); numDelivered >= uint64(hc.maxDeliver) {
		reason := fmt.Sprintf("job was delivered %d times without completing (max_deliver=%d); giving up", numDelivered, hc.maxDeliver)
		slog.Error("Giving up on job after repeated redeliveries",
			"workerID", workerID,
			"jobID", job.ID.String(),
			"priority", priority,
			"numDelivered", numDelivered)
		if err := publishJobStatus(hc.natsConnection, buildstatus.StatusFailed, job.ID.String(), reason); err != nil {
			slog.Error("Failed to publish terminal failure status", "error", err, "jobID", job.ID.String())
		}
		if err := msg.Term(); err != nil {
			slog.Error("Failed to terminate exhausted message", "error", err, "jobID", job.ID.String())
		}
		return
	}

	defer func() {
		if r := recover(); r != nil {
			slog.Error("Job processing panic",
				"workerID", workerID,
				"jobID", job.ID.String(),
				"panic", r)
			// NAK on panic so job can be retried after a delay
			if err := msg.NakWithDelay(5 * time.Second); err != nil {
				slog.Error("Failed to NAK message after panic", "error", err)
			}
			return
		}
	}()

	slog.Info("Worker starting job",
		"workerID", workerID,
		"jobID", job.ID.String(),
		"priority", priority)

	// processing(job) blocks for the whole job on the Docker executor (it waits
	// on every step's container). Without a heartbeat, any job longer than
	// AckWait is redelivered and executed a second time while the first copy is
	// still running - both copies then share the same shared-<jobID> volume.
	// Signalling in-progress resets the ack timer for as long as the job runs,
	// however long that legitimately is.
	//
	// The stop function is registered as a defer so a panic in processing also
	// stops it; it runs before the recover defer above (defers are LIFO) and so
	// before the NAK. It is also called explicitly below so no heartbeat can
	// race with the Ack.
	stopAckProgress := startAckProgress(msg.InProgress, ackProgressInterval(hc.ackWait), job.ID.String())
	defer stopAckProgress()

	processing(job)

	stopAckProgress()

	slog.Info("Worker finished job",
		"workerID", workerID,
		"jobID", job.ID.String(),
		"priority", priority)

	// Acknowledge after successful processing
	if err := msg.Ack(); err != nil {
		slog.Error("Failed to acknowledge message",
			"error", err,
			"jobID", job.ID.String())
	}
}

// deliveryCount reports how many times msg has been delivered, starting at 1
// for the first delivery. It returns 0 if the metadata is unreadable, which
// keeps an unexpected message shape from being terminated as poisonous.
func deliveryCount(msg jetstream.Msg, jobID string) uint64 {
	meta, err := msg.Metadata()
	if err != nil {
		slog.Warn("Failed to read message metadata", "error", err, "jobID", jobID)
		return 0
	}
	return meta.NumDelivered
}

// startAckProgress repeatedly calls inProgress (jetstream.Msg.InProgress) every
// interval until the returned stop function is called, resetting the message's
// AckWait timer while the job runs.
//
// The returned stop function is idempotent and waits for the goroutine to exit,
// so once it returns, inProgress is guaranteed not to be called again - an
// in-progress signal sent after Ack/Nak would be an error on a terminated
// message.
func startAckProgress(inProgress func() error, interval time.Duration, jobID string) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := inProgress(); err != nil {
					slog.Warn("Failed to signal job progress to JetStream", "error", err, "jobID", jobID)
				}
			}
		}
	}()

	return sync.OnceFunc(func() {
		close(done)
		<-stopped
	})
}

// parseMessage handles message parsing and validation
func (hc *HadesNATSConsumer) parseMessage(ctx context.Context, msg jetstream.Msg) (payload.QueuePayload, error) {
	var job payload.QueuePayload

	// Get the UUID from the message data
	msgID, err := uuid.FromBytes(msg.Data())
	if err != nil {
		slog.Error("Failed to parse message ID", "error", err, "data", string(msg.Data()))
		// Terminate message on parse error (won't be retried)
		if termErr := msg.Term(); termErr != nil {
			slog.Error("Failed to terminate message after parse error", "error", termErr)
		}
		return job, fmt.Errorf("parse message ID: %w", err)
	}

	entry, err := hc.kv.Get(ctx, msgID.String())
	if err != nil {
		// Handle missing payload specifically
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrBucketNotFound) {
			slog.Error("Job payload missing from KeyValue store", "id", msgID.String(), "error", err)
			if termErr := msg.Term(); termErr != nil {
				slog.Error("Failed to terminate message after missing payload", "error", termErr, "id", msgID.String())
			}
			return job, fmt.Errorf("job payload missing: %w", err)
		}
		slog.Error("Failed to get message from KeyValue store", "error", err, "id", msgID.String())
		// NAK with delay for transient KV errors
		if nakErr := msg.NakWithDelay(5 * time.Second); nakErr != nil {
			slog.Error("Failed to NAK message after KV store error", "error", nakErr)
		}
		return job, fmt.Errorf("get from KV store: %w", err)
	}

	if err := json.Unmarshal(entry.Value(), &job); err != nil {
		slog.Error("Failed to unmarshal message payload", "error", err, "id", msgID.String())
		// Terminate on unmarshal error (data corruption, won't be retried)
		if termErr := msg.Term(); termErr != nil {
			slog.Error("Failed to terminate message after unmarshal error", "error", termErr)
		}
		return job, fmt.Errorf("unmarshal payload: %w", err)
	}

	return job, nil
}

// fetchNextMessage tries to fetch a message in strict priority order
func (hc *HadesNATSConsumer) fetchNextMessage(ctx context.Context) (jetstream.Msg, hades.Priority, bool) {
	for _, p := range hades.Priorities {
		consumer := hc.consumers[p]

		// Use FetchNoWait with proper cleanup
		batch, err := consumer.FetchNoWait(1)
		if err != nil {
			// Only log non-timeout errors
			if err != jetstream.ErrNoMessages {
				slog.Error("Failed to fetch message", "error", err, "priority", p)
			}
			continue
		}

		// Properly drain the messages channel
		msgs := batch.Messages()
		select {
		case msg, ok := <-msgs:
			if ok {
				// Drain any remaining messages (should be none with batch size 1)
				go func() {
					for range msgs {
						// Drain to prevent goroutine leak
					}
				}()
				slog.Debug("Found message", "subject", msg.Subject(), "priority", p)
				return msg, p, true
			}
		case <-ctx.Done():
			return nil, "", false
		}
	}

	return nil, "", false
}
