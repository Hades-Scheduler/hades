package dashboard

import (
	"context"
	"slices"
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
	// identityKnown is set once name and step count came from TrackEnqueue or
	// a KV lookup (including a definitive miss), so the list skips the lookup.
	identityKnown bool
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
	rec.identityKnown = true
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

// trackerStats is the aggregate the metrics snapshot needs from the tracker.
type trackerStats struct {
	counts           map[string]int
	durations        []int64
	finishedRecently int
}

// stats returns per-status counts, completed-job durations in milliseconds and
// how many jobs finished within window, gathered in one pass under one lock.
func (t *tracker) stats(window time.Duration) trackerStats {
	cutoff := time.Now().Add(-window)
	t.mu.RLock()
	defer t.mu.RUnlock()
	st := trackerStats{counts: make(map[string]int)}
	for _, rec := range t.jobs {
		st.counts[rec.status.String()]++
		if rec.finishedAt.IsZero() {
			continue
		}
		if !rec.startedAt.IsZero() {
			st.durations = append(st.durations, rec.finishedAt.Sub(rec.startedAt).Milliseconds())
		}
		if rec.finishedAt.After(cutoff) {
			st.finishedRecently++
		}
	}
	return st
}

// needsIdentity reports whether jobID's name and step count are still unknown
// and have not been looked up yet.
func (t *tracker) needsIdentity(jobID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	rec, ok := t.jobs[jobID]
	return ok && !rec.identityKnown
}

// setIdentity records a job's name and step count learned from its stored
// payload. It is also called with empty values once a payload is known to be
// gone, so the job list does not look it up again on every request.
func (t *tracker) setIdentity(jobID, name string, stepCount int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.jobs[jobID]
	if !ok {
		return
	}
	rec.identityKnown = true
	if rec.name == "" {
		rec.name = name
	}
	if rec.stepCount == 0 {
		rec.stepCount = stepCount
	}
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

// enforceCapLocked evicts the oldest records while the map exceeds the cap. It
// sorts once rather than rescanning the map per evicted record, which after a
// burst would be quadratic in the overflow. Caller must hold the write lock.
func (t *tracker) enforceCapLocked() {
	if len(t.jobs) <= maxTrackedJobs {
		return
	}
	recs := make([]*jobRecord, 0, len(t.jobs))
	for _, rec := range t.jobs {
		recs = append(recs, rec)
	}
	slices.SortFunc(recs, func(a, b *jobRecord) int {
		return a.updatedAt.Compare(b.updatedAt)
	})
	for _, rec := range recs[:len(recs)-maxTrackedJobs] {
		delete(t.jobs, rec.id)
	}
}

// statusFromSubject extracts the status token from a "hades.jobstatus.X" subject.
func statusFromSubject(subject string) buildstatus.JobStatus {
	return buildstatus.StatusFromSubject(subject)
}
