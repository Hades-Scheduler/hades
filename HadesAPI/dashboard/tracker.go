package dashboard

import (
	"context"
	"strings"
	"sync"
	"time"

	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/buildstatus"
)

// cleanupInterval bounds how often finished jobs are swept from the tracker.
const cleanupInterval = time.Minute

// staleJobTTL is a hard ceiling after which even non-terminal jobs are evicted.
// A job stuck in Queued/Running (e.g. a lost terminal-status event or a hung
// job) would otherwise be retained forever.
const staleJobTTL = 24 * time.Hour

// maxTrackedJobs caps the tracker map; when exceeded, the oldest records are
// evicted so an adversarial or runaway submitter cannot exhaust memory.
const maxTrackedJobs = 10000

// JobSummary is the list/stream representation of a tracked job. Name and
// StepCount are populated by the list handler from the KV payload; the tracker
// itself fills the rest.
type JobSummary struct {
	ID         string     `json:"id"`
	Name       string     `json:"name,omitempty"`
	Priority   string     `json:"priority,omitempty"`
	Status     string     `json:"status"`
	Reason     string     `json:"reason,omitempty"`
	StepCount  int        `json:"stepCount,omitempty"`
	QueuedAt   *time.Time `json:"queuedAt,omitempty"`
	StartedAt  *time.Time `json:"startedAt,omitempty"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	DurationMs *int64     `json:"durationMs,omitempty"`
}

// jobRecord is the tracker's mutable per-job state.
type jobRecord struct {
	id         string
	name       string
	priority   hades.Priority
	stepCount  int
	status     buildstatus.JobStatus
	reason     string
	queuedAt   time.Time
	startedAt  time.Time
	finishedAt time.Time
	updatedAt  time.Time
}

func (r *jobRecord) dto() JobSummary {
	s := JobSummary{
		ID:        r.id,
		Name:      r.name,
		Priority:  string(r.priority),
		StepCount: r.stepCount,
		Status:    r.status.String(),
		Reason:    r.reason,
	}
	if !r.queuedAt.IsZero() {
		t := r.queuedAt
		s.QueuedAt = &t
	}
	if !r.startedAt.IsZero() {
		t := r.startedAt
		s.StartedAt = &t
	}
	if !r.finishedAt.IsZero() {
		t := r.finishedAt
		s.FinishedAt = &t
	}
	if !r.startedAt.IsZero() {
		end := r.finishedAt
		if end.IsZero() {
			end = time.Now()
		}
		ms := end.Sub(r.startedAt).Milliseconds()
		s.DurationMs = &ms
	}
	return s
}

// isTerminal reports whether a status ends a job's lifecycle.
func isTerminal(s buildstatus.JobStatus) bool {
	return s.IsTerminal()
}

// tracker holds recently-seen jobs in memory. It is fed by TrackEnqueue and by
// the live status subscription, and is swept of finished jobs after retention.
type tracker struct {
	mu        sync.RWMutex
	jobs      map[string]*jobRecord
	retention time.Duration
}

func newTracker(retention time.Duration) *tracker {
	if retention <= 0 {
		retention = time.Hour
	}
	return &tracker{jobs: make(map[string]*jobRecord), retention: retention}
}

// enqueue records a newly submitted job with its name, step count and priority.
func (t *tracker) enqueue(jobID, name string, stepCount int, priority hades.Priority) JobSummary {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	rec := t.jobs[jobID]
	if rec == nil {
		rec = &jobRecord{id: jobID}
		t.jobs[jobID] = rec
	}
	rec.name = name
	rec.priority = priority
	rec.stepCount = stepCount
	if rec.status == "" {
		rec.status = buildstatus.StatusQueued
	}
	if rec.queuedAt.IsZero() {
		rec.queuedAt = now
	}
	rec.updatedAt = now
	t.enforceInsertCapLocked()
	return rec.dto()
}

// observe applies a status transition learned from the live subscription. An
// optional reason (e.g. "ImagePullBackOff: ...") explaining the status is stored
// when present and retained across later empty-reason updates.
func (t *tracker) observe(jobID string, status buildstatus.JobStatus, reason string) JobSummary {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	rec := t.jobs[jobID]
	if rec == nil {
		rec = &jobRecord{id: jobID}
		t.jobs[jobID] = rec
		t.enforceInsertCapLocked()
	}
	rec.status = status
	if reason != "" {
		rec.reason = reason
	}
	rec.updatedAt = now
	switch status {
	case buildstatus.StatusQueued:
		if rec.queuedAt.IsZero() {
			rec.queuedAt = now
		}
	case buildstatus.StatusRunning:
		if rec.startedAt.IsZero() {
			rec.startedAt = now
		}
	default:
		if isTerminal(status) && rec.finishedAt.IsZero() {
			rec.finishedAt = now
		}
	}
	return rec.dto()
}

// get returns a copy of the summary for jobID, if tracked.
func (t *tracker) get(jobID string) (JobSummary, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	rec, ok := t.jobs[jobID]
	if !ok {
		return JobSummary{}, false
	}
	return rec.dto(), true
}

// list returns all tracked job summaries, optionally filtered by status.
func (t *tracker) list(statusFilter string) []JobSummary {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]JobSummary, 0, len(t.jobs))
	for _, rec := range t.jobs {
		if statusFilter != "" && !strings.EqualFold(rec.status.String(), statusFilter) {
			continue
		}
		out = append(out, rec.dto())
	}
	return out
}

// counts returns the number of tracked jobs per status string.
func (t *tracker) counts() map[string]int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	c := make(map[string]int)
	for _, rec := range t.jobs {
		c[rec.status.String()]++
	}
	return c
}

// finishedDurations returns completed-job durations in milliseconds.
func (t *tracker) finishedDurations() []int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var ds []int64
	for _, rec := range t.jobs {
		if !rec.startedAt.IsZero() && !rec.finishedAt.IsZero() {
			ds = append(ds, rec.finishedAt.Sub(rec.startedAt).Milliseconds())
		}
	}
	return ds
}

// finishedSince returns how many jobs reached a terminal state within window.
func (t *tracker) finishedSince(window time.Duration) int {
	cutoff := time.Now().Add(-window)
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := 0
	for _, rec := range t.jobs {
		if !rec.finishedAt.IsZero() && rec.finishedAt.After(cutoff) {
			n++
		}
	}
	return n
}

func (t *tracker) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.sweep()
		}
	}
}

// sweep removes finished jobs past retention, evicts jobs stuck in a
// non-terminal state past the hard stale ceiling, and enforces the map cap.
func (t *tracker) sweep() {
	now := time.Now()
	terminalCutoff := now.Add(-t.retention)
	staleCutoff := now.Add(-staleJobTTL)
	t.mu.Lock()
	defer t.mu.Unlock()

	for id, rec := range t.jobs {
		if isTerminal(rec.status) {
			if rec.updatedAt.Before(terminalCutoff) {
				delete(t.jobs, id)
			}
		} else if rec.updatedAt.Before(staleCutoff) {
			delete(t.jobs, id)
		}
	}

	t.enforceCapLocked()
}

// enforceInsertCapLocked bounds the map between sweeps: if a burst pushes it
// past twice the cap, evict back down to the cap. The 2x hysteresis amortizes
// the O(n) eviction across many inserts instead of running it on every insert
// at steady state. Caller must hold the write lock.
func (t *tracker) enforceInsertCapLocked() {
	if len(t.jobs) > 2*maxTrackedJobs {
		t.enforceCapLocked()
	}
}

// enforceCapLocked evicts the oldest records while the map exceeds the cap.
// Caller must hold the write lock.
func (t *tracker) enforceCapLocked() {
	for len(t.jobs) > maxTrackedJobs {
		var oldestID string
		var oldest time.Time
		first := true
		for id, rec := range t.jobs {
			if first || rec.updatedAt.Before(oldest) {
				oldestID, oldest, first = id, rec.updatedAt, false
			}
		}
		delete(t.jobs, oldestID)
	}
}

// statusFromSubject extracts the status token from a "hades.jobstatus.X" subject.
func statusFromSubject(subject string) buildstatus.JobStatus {
	return buildstatus.StatusFromSubject(subject)
}
