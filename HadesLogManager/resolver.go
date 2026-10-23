package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// logTarget is the subset of a stored job payload the log path needs: where to
// forward the aggregated logs and how long the job may run, which bounds how long
// its log watcher is kept alive.
type logTarget struct {
	CallbackURL    string
	TimeoutSeconds int64
}

// jobCallbackResolver resolves the log-forwarding target for a given job ID.
type jobCallbackResolver interface {
	LogTarget(ctx context.Context, jobID string) (logTarget, error)
}

// jobInfo is the subset of a stored job payload the status webhook needs.
// Found reports whether a payload was stored for the job at all, which lets the
// caller distinguish "job unknown" (nothing to send) from "job known, no status
// callback configured".
type jobInfo struct {
	Found             bool
	Name              string
	StatusCallbackURL string
}

// jobInfoResolver resolves the status-webhook-relevant fields of a job payload.
// It is deliberately separate from jobCallbackResolver so the status webhook and
// the log forwarding path stay independent.
type jobInfoResolver interface {
	JobInfo(ctx context.Context, jobID string) (jobInfo, error)
}

// kvCallbackResolver resolves the callback URL by reading the job payload from
// the HADES_JOBS JetStream key-value store, where HadesAPI stores every job
// keyed by its ID. This is the same bucket the scheduler consumes from.
type kvCallbackResolver struct {
	kv jetstream.KeyValue
}

// newKVCallbackResolver creates a resolver backed by the given KV store.
func newKVCallbackResolver(kv jetstream.KeyValue) *kvCallbackResolver {
	return &kvCallbackResolver{kv: kv}
}

// LogTarget returns the callback URL and timeout stored on the job payload for
// jobID. A missing key or bucket yields a zero logTarget and no error so callers
// treat it as "no callback configured" rather than an error. Read or unmarshal
// failures are returned as errors.
func (r *kvCallbackResolver) LogTarget(ctx context.Context, jobID string) (logTarget, error) {
	job, found, err := r.load(ctx, jobID)
	if err != nil || !found {
		return logTarget{}, err
	}
	return logTarget{CallbackURL: job.CallbackURL, TimeoutSeconds: job.TimeoutSeconds}, nil
}

// JobInfo returns the job name and status-callback URL stored for jobID. A
// missing key or bucket yields a zero jobInfo with Found=false and no error, so
// callers treat an unknown job as "nothing to notify".
func (r *kvCallbackResolver) JobInfo(ctx context.Context, jobID string) (jobInfo, error) {
	job, found, err := r.load(ctx, jobID)
	if err != nil || !found {
		return jobInfo{}, err
	}
	return jobInfo{Found: true, Name: job.Name, StatusCallbackURL: job.StatusCallbackURL}, nil
}

// storedJob mirrors the few payload.QueuePayload fields this service reads.
// Decoding into it skips the steps, scripts and metadata maps, which make up
// nearly all of a payload and are never needed here.
type storedJob struct {
	Name              string `json:"name"`
	CallbackURL       string `json:"callback_url"`
	StatusCallbackURL string `json:"status_callback_url"`
	TimeoutSeconds    int64  `json:"timeout_seconds"`
}

// load reads and decodes the job payload stored under jobID. A missing key or
// bucket is reported as (zero, false, nil); read or decode failures are errors.
func (r *kvCallbackResolver) load(ctx context.Context, jobID string) (storedJob, bool, error) {
	var job storedJob

	entry, err := r.kv.Get(ctx, jobID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrBucketNotFound) {
			return job, false, nil
		}
		return job, false, fmt.Errorf("reading job payload from KV store: %w", err)
	}

	if err := json.Unmarshal(entry.Value(), &job); err != nil {
		return job, false, fmt.Errorf("unmarshaling job payload: %w", err)
	}

	return job, true, nil
}
