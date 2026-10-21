package nats

import (
	"fmt"

	hades "github.com/hades-scheduler/hades/shared"
)

// natsSubjectBase is the base NATS subject for all job messages
const natsSubjectBase = "hades.jobs"

// allJobsSubject matches all priority subjects. The job stream and the consumer's
// wake-up subscription both use it; if they drifted apart, wake-ups would stop
// without any error and workers would only poll.
const allJobsSubject = natsSubjectBase + ".*"

// prioritySubject returns the NATS subject for a given priority level.
func prioritySubject(p hades.Priority) string {
	return fmt.Sprintf("%s.%s", natsSubjectBase, p)
}
