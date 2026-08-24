package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/nats-io/nats.go/jetstream"
)

// jobCallbackResolver resolves the per-job callback URL for a given job ID.
type jobCallbackResolver interface {
	CallbackURL(ctx context.Context, jobID string) (string, error)
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

// CallbackURL returns the callback URL stored on the job payload for jobID.
// A missing key or bucket yields ("", nil) so callers treat it as "no callback
// configured" rather than an error. Read or unmarshal failures are returned as
// errors.
func (r *kvCallbackResolver) CallbackURL(ctx context.Context, jobID string) (string, error) {
	job, found, err := r.load(ctx, jobID)
	if err != nil || !found {
		return "", err
	}
	return job.CallbackURL, nil
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

// load reads and decodes the job payload stored under jobID. A missing key or
// bucket is reported as (zero, false, nil); read or decode failures are errors.
func (r *kvCallbackResolver) load(ctx context.Context, jobID string) (payload.QueuePayload, bool, error) {
	var job payload.QueuePayload

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
