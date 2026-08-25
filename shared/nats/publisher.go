package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	_ hades.JobPublisher          = (*HadesNATSPublisher)(nil)
	_ buildstatus.StatusPublisher = (*HadesNATSPublisher)(nil)
)

// HadesNATSPublisher manages job publishing to the NATS queue system.
type HadesNATSPublisher struct {
	natsConnection *nats.Conn
	js             jetstream.JetStream
	kv             jetstream.KeyValue
}

// NewHadesPublisher creates a new job producer with JetStream support.
// It initializes the job stream and key-value store for job metadata.
func NewHadesPublisher(nc *nats.Conn) (*HadesNATSPublisher, error) {
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("Failed to create JetStream context", "error", err)
		return nil, err
	}

	s, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       "HADES_JOBS",
		Subjects:   []string{fmt.Sprintf("%s.*", natsSubjectBase)},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.WorkQueuePolicy,
		Duplicates: 1 * time.Minute, // Disallow duplicates for 1 minute
		MaxMsgs:    -1,
		MaxAge:     24 * time.Hour, // Retain jobs for 24 hours by default
	})
	if err != nil {
		slog.Error("Failed to create JetStream stream", "error", err)
		return nil, err
	}
	slog.Info("Created JetStream stream", "stream", s)

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "HADES_JOBS",
	})
	if err != nil {
		slog.Error("Failed to create JetStream KeyValue store", "error", err)
		return nil, err
	}

	return &HadesNATSPublisher{
		natsConnection: nc,
		js:             js,
		kv:             kv,
	}, nil
}

// EnqueueHighJob adds a job to the high-priority queue.
func (hp *HadesNATSPublisher) EnqueueHighJob(ctx context.Context, queuePayload payload.QueuePayload) error {
	return hp.EnqueueJobWithPriority(ctx, queuePayload, hades.HighPriority)
}

// EnqueueMediumJob adds a job to the medium-priority queue.
func (hp *HadesNATSPublisher) EnqueueMediumJob(ctx context.Context, queuePayload payload.QueuePayload) error {
	return hp.EnqueueJobWithPriority(ctx, queuePayload, hades.MediumPriority)
}

// EnqueueLowJob adds a job to the low-priority queue.
func (hp *HadesNATSPublisher) EnqueueLowJob(ctx context.Context, queuePayload payload.QueuePayload) error {
	return hp.EnqueueJobWithPriority(ctx, queuePayload, hades.LowPriority)
}

// EnqueueJobWithPriority adds a job to the queue with the specified priority.
// The job payload is stored in the key-value store and a reference is published to the stream.
func (hp *HadesNATSPublisher) EnqueueJobWithPriority(ctx context.Context, queuePayload payload.QueuePayload, priority hades.Priority) error {
	bytesPayload, err := json.Marshal(queuePayload)
	if err != nil {
		slog.Error("Failed to marshal payload", "error", err)
		return fmt.Errorf("failed to marshal job payload: %w", err)
	}
	// Store full job payload in key-value store first
	_, err = hp.kv.Put(ctx, queuePayload.ID.String(), bytesPayload)
	if err != nil {
		return fmt.Errorf("failed to store job payload in KV store: %w", err)
	}
	// Publish job reference to the stream (use synchronous publish for guaranteed delivery)
	_, err = hp.js.Publish(ctx, prioritySubject(priority), queuePayload.ID[:], jetstream.WithMsgID(queuePayload.ID.String()))
	if err != nil {
		return fmt.Errorf("failed to publish job to stream: %w", err)
	}
	return nil
}

// PublishJobStatus publishes a job status change on the core-NATS status
// subject "hades.jobstatus.{status}". Subscribers such as HadesLogManager use
// these events to track a job's lifecycle. Nothing publishes the Queued status
// today except the API on enqueue, so this completes the lifecycle feed.
func (hp *HadesNATSPublisher) PublishJobStatus(_ context.Context, jobStatus buildstatus.JobStatus, jobID string, reason ...string) error {
	return publishJobStatus(hp.natsConnection, jobStatus, jobID, reason...)
}

// publishJobStatus publishes a status transition on the core-NATS status
// subject "hades.jobstatus.{status}". It backs HadesNATSPublisher's
// StatusPublisher implementation and is also used by the consumer to report a
// job it gives up on, so both report failures the same way.
func publishJobStatus(nc *nats.Conn, jobStatus buildstatus.JobStatus, jobID string, reason ...string) error {
	if jobID == "" {
		return fmt.Errorf("empty job ID")
	}
	if !jobStatus.IsValid() {
		return fmt.Errorf("invalid job status: %s", jobStatus)
	}
	subject := buildstatus.StatusSubject(jobStatus)
	// Payload stays the bare job ID; an optional reason rides in a header so
	// payload-only subscribers (HadesLogManager) are unaffected.
	msg := &nats.Msg{Subject: subject, Data: []byte(jobID)}
	if r := buildstatus.FirstReason(reason...); r != "" {
		msg.Header = nats.Header{buildstatus.ReasonHeader: []string{r}}
	}
	if err := nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("publishing job status %s for job %s: %w", jobStatus, jobID, err)
	}
	slog.Debug("Published job status", "job_id", jobID, "status", jobStatus, "subject", subject)
	return nil
}
