// Package buildstatus defines the job lifecycle status enum and the NATS
// subjects used to publish status transitions. Status events are published on
// "hades.jobstatus.<Status>" with the job ID as the message payload;
// HadesLogManager subscribes to these to start and stop watching a job's logs.
package buildstatus

import (
	"context"
	"fmt"
	"strings"
)

// JobStatus is a point in a job's lifecycle. Its string value is capitalized and
// is used verbatim as the last token of the status NATS subject (see
// StatusSubjectFormat), so the constants below and any subject matching must stay
// in sync.
type JobStatus string

// StatusPublisher publishes job status transitions to NATS JetStream. An
// optional reason may accompany a status (typically a terminal Failed) to
// explain it, e.g. "ImagePullBackOff: ...". Only the first reason is used.
type StatusPublisher interface {
	PublishJobStatus(ctx context.Context, status JobStatus, jobID string, reason ...string) error
}

// ReasonHeader is the NATS message header carrying an optional human-readable
// reason for a status transition (e.g. why a job Failed). The message payload
// stays the bare job ID for backward compatibility, so subscribers that only
// read the payload (e.g. HadesLogManager) are unaffected.
const ReasonHeader = "X-Hades-Reason"

// The job lifecycle statuses. Queued and Running are transient; Succeeded,
// Failed, and Stopped are terminal.
const (
	StatusQueued    JobStatus = "Queued"
	StatusRunning   JobStatus = "Running"
	StatusSucceeded JobStatus = "Succeeded"
	StatusFailed    JobStatus = "Failed"
	StatusStopped   JobStatus = "Stopped"
)

// StatusSubjectFormat is the NATS subject template for status events; format it
// with a JobStatus (e.g. "hades.jobstatus.Running") via StatusSubject.
const StatusSubjectFormat = "hades.jobstatus.%s"

// String returns the status as its underlying string value.
func (js JobStatus) String() string {
	return string(js)
}

// IsValid reports whether js is one of the defined status constants.
func (js JobStatus) IsValid() bool {
	switch js {
	case StatusQueued, StatusRunning, StatusSucceeded, StatusFailed, StatusStopped:
		return true
	default:
		return false
	}
}

// IsTerminal reports whether js ends a job's lifecycle. Succeeded, Failed, and
// Stopped are terminal; Queued and Running are not.
func (js JobStatus) IsTerminal() bool {
	switch js {
	case StatusSucceeded, StatusFailed, StatusStopped:
		return true
	default:
		return false
	}
}

// StatusSubject returns the NATS subject for publishing the given status.
func StatusSubject(status JobStatus) string {
	return fmt.Sprintf(StatusSubjectFormat, status)
}

// StatusFromSubject extracts the status token from a "hades.jobstatus.X"
// subject. It returns the empty JobStatus when the subject has no trailing
// token; callers should check IsValid on the result.
func StatusFromSubject(subject string) JobStatus {
	idx := strings.LastIndex(subject, ".")
	if idx < 0 || idx == len(subject)-1 {
		return JobStatus("")
	}
	return JobStatus(subject[idx+1:])
}

// FirstReason returns the first non-empty reason from a variadic reason list,
// or "" if none was provided. Publishers use it to normalize the optional
// PublishJobStatus reason argument.
func FirstReason(reason ...string) string {
	for _, r := range reason {
		if r != "" {
			return r
		}
	}
	return ""
}

// MaxReasonLen is the hard upper bound, in runes, on a status reason. It keeps
// a verbose executor or controller message within NATS header limits and stops
// it bloating anything that forwards it onwards. TruncateReason never returns
// more than this, ellipsis included.
const MaxReasonLen = 500

// TruncateReason shortens reason so the result is at most MaxReasonLen runes,
// with the final rune replaced by an ellipsis to mark that content was dropped.
// Publishers should apply it before attaching a reason to a status event, and
// consumers that forward a reason outside the cluster should apply it again:
// not every publisher does, so the cap only holds where the boundary that
// depends on it enforces it. It is idempotent on an already-truncated value.
func TruncateReason(reason string) string {
	runes := []rune(reason)
	if len(runes) <= MaxReasonLen {
		return reason
	}
	return string(runes[:MaxReasonLen-1]) + "…"
}
