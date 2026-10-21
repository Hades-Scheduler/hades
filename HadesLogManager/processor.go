package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/utils"
)

const (
	httpClientTimeout = 10 * time.Second
	// defaultRetention is used when LOG_RETENTION is unset or non-positive. A
	// non-positive retention would otherwise make completed jobs eligible for
	// flushing immediately.
	defaultRetention = time.Hour
	// staleJobTTL evicts jobs that never reached a terminal status (a lost
	// terminal event, or a job seen only as Queued). It matches the queue
	// retention, so a job seen only as Queued for that long can no longer start.
	// A job still running past it loses only its in-memory copy: its terminal
	// status replays the logs from JetStream.
	staleJobTTL = 24 * time.Hour
)

// NATSLogAggregator implements LogAggregator using in-memory storage for fast log retrieval.
// It provides thread-safe log aggregation with configurable batching, automatic log rotation,
// and memory management. Thread-safety is provided by sync.Map for all operations.
type NATSLogAggregator struct {
	hlc       *buildlogs.HadesLogConsumer
	resolver  jobCallbackResolver
	logs      sync.Map // jobID (string) -> logsVersion
	completed sync.Map // jobID (string) -> time.Time (completion time)
	status    sync.Map // jobID (string) -> statusEntry
	targets   sync.Map // jobID (string) -> logTarget, resolved when the watcher starts
	client    *http.Client
	cfg       AggregatorConfig
}

// statusEntry records when a status was last set so jobs that never complete
// can still be swept.
type statusEntry struct {
	status    buildstatus.JobStatus
	updatedAt time.Time
}

// wrapper stored in sync.Map: comparable (uint64 + pointer)
type logsVersion struct {
	ver uint64
	ptr *[]buildlogs.Log
}

// AggregatorConfig defines the configuration parameters for log aggregation behavior.
type AggregatorConfig struct {
	BatchSize  int           `env:"LOG_BATCH_SIZE" envDefault:"100"`
	Retention  time.Duration `env:"LOG_RETENTION" envDefault:"1h"`
	MaxJobLogs int           `env:"MAX_JOB_LOGS" envDefault:"1000"`
}

// NewLogAggregator creates a new NATS-based LogAggregator instance with the specified configuration.
// It starts a background goroutine for periodic cleanup of completed jobs.
//
// Parameters:
//   - ctx: Context for controlling the lifecycle of background goroutines
//   - hlc: HadesLogConsumer instance (currently unused but kept for future extensibility)
//   - resolver: resolves the per-job callback URL used when forwarding logs; may be nil
//   - client: HTTP client for the log callback; nil uses one with httpClientTimeout
//   - config: AggregatorConfig containing batching and limit settings
//
// Returns:
//   - LogAggregator: A new instance ready to aggregate logs
func NewLogAggregator(ctx context.Context, hlc *buildlogs.HadesLogConsumer, resolver jobCallbackResolver, client *http.Client, config AggregatorConfig) *NATSLogAggregator {
	// Normalize retention once so every downstream use (cleanup cadence and the
	// expiry comparison) works from a single, guaranteed-positive value.
	if config.Retention <= 0 {
		slog.Warn("Non-positive LOG_RETENTION; falling back to default",
			"configured", config.Retention, "default", defaultRetention)
		config.Retention = defaultRetention
	}

	if client == nil {
		client = &http.Client{Timeout: httpClientTimeout}
	}

	la := &NATSLogAggregator{
		hlc:      hlc,
		resolver: resolver,
		client:   client,
		cfg:      config,
	}

	// Start background cleanup goroutine
	go la.cleanupLoop(ctx)

	return la
}

// cleanupLoop runs periodic cleanup of completed jobs. It ticks at a quarter of
// the retention (floored at one minute): ticking once per retention would let an
// entry that expired just after a tick live for almost two retention periods.
func (la *NATSLogAggregator) cleanupLoop(ctx context.Context) {
	interval := la.cfg.Retention / 4
	if interval < time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Stopping log aggregator cleanup goroutine")
			return
		case <-ticker.C:
			la.cleanupCompletedJobs()
			la.cleanupStaleJobs(time.Now())
		}
	}
}

// AddLog merges an incoming log into the aggregator for the specified job.
//
// Logs are coalesced by ContainerID so that the stored slice holds exactly one
// buildlogs.Log per container, in first-seen order. Under live streaming a
// container emits many small Logs over its lifetime; appending each as a
// separate element would break the positional logs[index] == step contract that
// the Artemis adapter relies on. A zero-entry Log still registers the
// container's slot, so a step producing no output keeps its position.
//
// This method is thread-safe using sync.Map with compare-and-swap operations.
// It preserves copy-on-write semantics: the entries of the merged container are
// deep-copied so a concurrent GetJobLogs/SendJobLogs reader holding the previous
// slice never observes a mutated backing array.
//
// Parameters:
//   - log: The buildlogs.Log entry to add, must contain a valid JobID
func (la *NATSLogAggregator) AddLog(log buildlogs.Log) {
	if log.JobID == "" {
		slog.Warn("Attempted to add log with empty job ID")
		return
	}
	if log.ContainerID == "" {
		slog.Warn("Attempted to add log with empty container ID", "job_id", log.JobID)
		return
	}

	slog.Debug("Adding log to aggregator", "job_id", log.JobID, "container_id", log.ContainerID, "entries", len(log.Logs))

	// Use LoadOrStore and CompareAndSwap for thread-safe updates. Load first so
	// the empty placeholder is only allocated for a job's very first log.
	for {
		value, ok := la.logs.Load(log.JobID)
		if !ok {
			value, _ = la.logs.LoadOrStore(log.JobID, logsVersion{ver: 0, ptr: &[]buildlogs.Log{}})
		}
		old := value.(logsVersion)
		existing := *old.ptr

		// Shallow-copy the outer slice. Elements we do not touch stay shared with
		// readers (their entries are never mutated in place); only the merged
		// container element is replaced with a freshly allocated entries slice.
		newLogs := make([]buildlogs.Log, len(existing), len(existing)+1)
		copy(newLogs, existing)

		idx := -1
		for i := range newLogs {
			if newLogs[i].ContainerID == log.ContainerID {
				idx = i
				break
			}
		}

		if idx == -1 {
			// First time we see this container: register its slot with a copy of
			// the incoming entries so we never alias the caller's slice.
			merged := append([]buildlogs.LogEntry(nil), log.Logs...)
			merged = la.trimContainerEntries(log.JobID, log.ContainerID, merged)
			newLogs = append(newLogs, buildlogs.Log{JobID: log.JobID, ContainerID: log.ContainerID, Logs: merged})
		} else {
			// Merge into the existing container. Deep-copy the entries into a new
			// backing array so readers of the old slice are unaffected.
			prev := newLogs[idx].Logs
			merged := make([]buildlogs.LogEntry, 0, len(prev)+len(log.Logs))
			merged = append(merged, prev...)
			merged = append(merged, log.Logs...)
			merged = la.trimContainerEntries(log.JobID, log.ContainerID, merged)
			newLogs[idx] = buildlogs.Log{JobID: log.JobID, ContainerID: log.ContainerID, Logs: merged}
		}

		newVal := logsVersion{ver: old.ver + 1, ptr: &newLogs}

		// Atomically swap if the value hasn't changed
		if la.logs.CompareAndSwap(log.JobID, old, newVal) {
			slog.Debug("Merged log into aggregator", "job_id", log.JobID, "container_id", log.ContainerID, "containers", len(newLogs))
			break
		}
		// If swap failed, another goroutine modified the value - retry
	}
}

// trimContainerEntries caps a single container's entries at MaxJobLogs, dropping
// the oldest entries of that container. It never drops a whole container element,
// which would shift step indices and break the Artemis logs[index] contract.
func (la *NATSLogAggregator) trimContainerEntries(jobID, containerID string, entries []buildlogs.LogEntry) []buildlogs.LogEntry {
	if la.cfg.MaxJobLogs > 0 && len(entries) > la.cfg.MaxJobLogs {
		trimStart := len(entries) - la.cfg.MaxJobLogs
		slog.Debug("Trimmed old container entries", "job_id", jobID, "container_id", containerID, "trimmed_count", trimStart)
		entries = entries[trimStart:]
	}
	return entries
}

// FlushJob removes logs and status for a completed job from memory.
// This is called after the retention period expires to free up memory.
//
// Parameters:
//   - jobID: The unique identifier for the job whose data should be flushed
//
// Returns:
//   - error: Always nil in current implementation
func (la *NATSLogAggregator) FlushJob(jobID string) error {
	value, logsExists := la.logs.LoadAndDelete(jobID)
	if logsExists {
		v := value.(logsVersion)
		logs := *v.ptr
		slog.Info("Flushed job logs", "job_id", jobID, "batch_count", len(logs))
	} else {
		slog.Debug("No logs to flush for job", "job_id", jobID)
	}

	_, statusExists := la.status.LoadAndDelete(jobID)
	if !statusExists {
		slog.Debug("No status to flush for job", "job_id", jobID)
	}

	la.completed.Delete(jobID)
	la.targets.Delete(jobID)
	return nil
}

// ResetJobLogs drops the aggregated logs of jobID ahead of a watcher that
// re-reads the job's log subject from the start.
func (la *NATSLogAggregator) ResetJobLogs(jobID string) {
	la.logs.Delete(jobID)
}

// MarkJobCompleted marks a job as completed and schedules it for cleanup after retention period.
func (la *NATSLogAggregator) MarkJobCompleted(jobID string) {
	la.completed.Store(jobID, time.Now())
	slog.Info("Marked job as completed", "job_id", jobID, "retention", la.cfg.Retention)
}

// cleanupCompletedJobs removes logs for jobs that have exceeded the retention period.
func (la *NATSLogAggregator) cleanupCompletedJobs() {
	now := time.Now()
	cleanedCount := 0

	la.completed.Range(func(key, value any) bool {
		jobID := key.(string)
		completedAt := value.(time.Time)

		if now.Sub(completedAt) >= la.cfg.Retention {
			slog.Debug("Retention expired, flushing job", "job_id", jobID)

			if err := la.FlushJob(jobID); err != nil {
				slog.Error("Failed to flush job during cleanup", "job_id", jobID, "error", err)
			} else {
				cleanedCount++
			}
		}
		return true // Continue iteration
	})

	if cleanedCount > 0 {
		slog.Info("Completed log cleanup cycle",
			"cleaned_jobs", cleanedCount)
	}
}

// cleanupStaleJobs flushes jobs that were never marked completed and whose status
// has not changed for staleJobTTL, so a lost terminal event cannot pin a job in
// memory forever. Completed jobs are left to the retention sweep.
func (la *NATSLogAggregator) cleanupStaleJobs(now time.Time) {
	cutoff := now.Add(-staleJobTTL)
	cleaned := 0
	la.status.Range(func(key, value any) bool {
		jobID := key.(string)
		if _, done := la.completed.Load(jobID); done {
			return true
		}
		if value.(statusEntry).updatedAt.Before(cutoff) {
			_ = la.FlushJob(jobID)
			cleaned++
		}
		return true
	})
	if cleaned > 0 {
		slog.Warn("Flushed jobs that never completed", "count", cleaned, "ttl", staleJobTTL)
	}
}

// PrefetchJob resolves and caches the job's log target so the KV lookup runs
// while the job executes instead of between its terminal status and the log
// callback. A failed lookup is not cached; SendJobLogs then retries it.
func (la *NATSLogAggregator) PrefetchJob(ctx context.Context, jobID string) (logTarget, bool) {
	if la.resolver == nil {
		return logTarget{}, false
	}
	lookupCtx, cancel := context.WithTimeout(ctx, httpClientTimeout)
	defer cancel()
	target, err := la.resolver.LogTarget(lookupCtx, jobID)
	if err != nil {
		slog.Debug("Failed to prefetch log target", "job_id", jobID, "error", err)
		return logTarget{}, false
	}
	la.targets.Store(jobID, target)
	return target, true
}

// SendJobLogs retrieves all stored logs for jobID and POSTs them as JSON to the
// job's own callback URL, resolved from the job payload. Forwarding is per-job:
// if the job has no callback URL (or it cannot be resolved/validated), the call
// is a no-op. It returns an error only when the resolved endpoint is present and
// valid but the POST itself fails (request creation, transport, or a non-2xx
// response).
//
// The KV lookup and the HTTP POST run under a bounded context derived from ctx
// so a slow store or endpoint cannot stall graceful shutdown.
func (la *NATSLogAggregator) SendJobLogs(ctx context.Context, jobID string) error {
	sendCtx, cancel := context.WithTimeout(ctx, httpClientTimeout)
	defer cancel()

	if la.resolver == nil {
		slog.Debug("No callback resolver configured, skipping log forwarding", "job_id", jobID)
		return nil
	}

	var endpoint string
	if cached, ok := la.targets.Load(jobID); ok {
		endpoint = cached.(logTarget).CallbackURL
	} else {
		target, err := la.resolver.LogTarget(sendCtx, jobID)
		if err != nil {
			slog.Warn("Failed to resolve callback URL, skipping log forwarding", "job_id", jobID, "error", err)
			return nil
		}
		endpoint = target.CallbackURL
	}
	if endpoint == "" {
		slog.Debug("No callback URL for job, skipping log forwarding", "job_id", jobID)
		return nil
	}
	if err := utils.ValidateCallbackURL(endpoint); err != nil {
		slog.Warn("Invalid callback URL for job, skipping log forwarding", "job_id", jobID, "error", err)
		return nil
	}

	logs := la.GetJobLogs(jobID)

	jsonData, err := json.Marshal(logs)
	if err != nil {
		return fmt.Errorf("marshaling logs to JSON: %w", err)
	}
	slog.Debug("Marshaled logs to JSON", "job_id", jobID)

	req, err := http.NewRequestWithContext(sendCtx, http.MethodPost, endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("creating HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := la.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending HTTP request: %w", err)
	}
	defer drainAndClose(resp)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("callback endpoint returned non-success status %s for job %s", resp.Status, jobID)
	}

	slog.Info("Sent job logs to callback endpoint", "job_id", jobID, "status", resp.Status)
	return nil
}

// GetJobLogs retrieves all log entries for a specific job ID by flattening
// the batched logs into a single slice.
//
// This method is thread-safe using sync.Map.Load.
//
// Parameters:
//   - jobID: The unique identifier for the job whose logs to retrieve
//
// Returns:
//   - []buildlogs.Log: All logs of each container for the specified job, or empty slice if not found
func (la *NATSLogAggregator) GetJobLogs(jobID string) []buildlogs.Log {
	value, exists := la.logs.Load(jobID)
	if !exists {
		return []buildlogs.Log{}
	}

	v := value.(logsVersion)
	logs := *v.ptr
	return logs
}

// GetAllJobs returns a slice containing all job IDs that are running or completed
//
// This method is thread-safe using sync.Map.Range.
//
// Returns:
//   - []string: A slice of all job IDs currently stored in the aggregator
func (la *NATSLogAggregator) GetAllJobs() []string {
	jobs := make([]string, 0)

	la.status.Range(func(key, value any) bool {
		jobs = append(jobs, key.(string))
		return true // Continue iteration
	})

	return jobs
}

// UpdateJobStatus stores or overwrites the build status for jobID.
// This method is thread-safe via sync.Map.Store.
func (la *NATSLogAggregator) UpdateJobStatus(jobID string, status buildstatus.JobStatus) {
	la.status.Store(jobID, statusEntry{status: status, updatedAt: time.Now()})
}

// GetJobStatus returns the string representation of the current build status for jobID.
// It returns an error if no status has been recorded for the given job.
func (la *NATSLogAggregator) GetJobStatus(jobID string) (buildstatus.JobStatus, error) {
	value, exists := la.status.Load(jobID)
	if !exists {
		return buildstatus.JobStatus(""), fmt.Errorf("job not found: %s", jobID)
	}
	return value.(statusEntry).status, nil
}
