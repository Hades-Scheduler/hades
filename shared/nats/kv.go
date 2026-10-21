package nats

import (
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// JobsBucket is the JetStream KV bucket holding every job's full payload,
// keyed by job ID. HadesAPI writes it, the scheduler reads it on dequeue, and
// HadesLogManager and the dashboard read it for callback URLs and job details.
const JobsBucket = "HADES_JOBS"

// jobPayloadTTL is how long a job payload is kept after it is enqueued. It
// must outlast the whole job, not just its time in the queue: the payload is
// read again at the terminal status for the status webhook (including retries)
// and for the log callback, and by the dashboard detail view. Seven days covers
// the 24h queue retention, a long run and the webhook retry window with room to
// spare, while still bounding the bucket.
const jobPayloadTTL = 7 * 24 * time.Hour

// JobsKVConfig is the one definition of the HADES_JOBS bucket. Every service
// calls CreateOrUpdateKeyValue with it, and CreateOrUpdate rewrites the bucket
// to whatever the caller passes, so any drift between call sites would flip the
// bucket's settings depending on which service started last.
func JobsKVConfig() jetstream.KeyValueConfig {
	return jetstream.KeyValueConfig{
		Bucket: JobsBucket,
		TTL:    jobPayloadTTL,
	}
}
