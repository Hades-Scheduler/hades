package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	logs "github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/nats-io/nats.go"
)

var (
	// ErrEmptyJobID is returned when job ID is empty
	ErrEmptyJobID = errors.New("empty job ID")
)

// DynamicLogManager manages dynamic subscription to job logs based on job status changes.
// It automatically starts watching logs when a job begins executing and stops when the job
// completes - succeeds or fails. The manager maintains a map of active watchers to prevent
// duplicate subscriptions and ensure proper cleanup.
type DynamicLogManager struct {
	nc            *nats.Conn
	logConsumer   logs.LogConsumer
	logAggregator watchedAggregator
	// maxLifetime is the watcher lifetime for jobs without a known timeout.
	maxLifetime time.Duration
	mu          sync.RWMutex
	watchers    map[string]watcherState // jobID -> watcher state
	// finalized records jobs whose terminal status was already handled, so a
	// duplicate terminal event cannot forward the logs a second time (a terminal
	// without a live watcher replays them).
	// Guarded by mu; swept after staleJobTTL.
	finalized map[string]time.Time
	// stopping is set once shutdown starts waiting on sendWG; after that no new
	// forwarding goroutine may be added. Guarded by mu.
	stopping bool
	sendWG   sync.WaitGroup // tracks in-flight log-forwarding goroutines
}

// LogWatcherConfig bounds how long a job's log watcher may live.
type LogWatcherConfig struct {
	// MaxLifetime caps a watcher whose job has no timeout_seconds. A watcher
	// normally stops on the job's terminal status, but that is a core-NATS
	// event and can be missed; without a cap the watcher would live forever.
	MaxLifetime time.Duration `env:"LOG_WATCHER_MAX_LIFETIME" envDefault:"6h"`
}

const (
	// defaultWatcherMaxLifetime mirrors the LOG_WATCHER_MAX_LIFETIME default.
	defaultWatcherMaxLifetime = 6 * time.Hour
	// watcherLifetimeSlack is added to a job's own timeout so a job that runs
	// right up to it still has time to be scheduled, finish and drain.
	watcherLifetimeSlack = 10 * time.Minute
)

// watchedAggregator is the aggregator a DynamicLogManager feeds.
type watchedAggregator interface {
	logs.LogAggregator
	// ResetJobLogs drops a job's aggregated logs. A watcher always reads the
	// job's subject from the start, so whatever a previous watcher aggregated
	// has to go first or every line would appear twice.
	ResetJobLogs(jobID string)
	// PrefetchJob resolves a job's log target ahead of its completion.
	PrefetchJob(ctx context.Context, jobID string) (logTarget, bool)
}

// watcherState holds the state for a single job watcher
type watcherState struct {
	ctx    context.Context
	cancel context.CancelFunc
	// drain is closed to request a graceful stop: the watcher keeps consuming
	// until it is caught up to the stream before returning, so the in-memory
	// aggregation is complete. cancel remains the hard stop (shutdown/replacement).
	drain chan struct{}
	wg    *sync.WaitGroup
	// expired marks a watcher cancelled by its lifetime limit. Its logs are
	// partial, so the job's terminal status must replay them, not drain it.
	expired bool
}

// NewDynamicLogManager creates a new DynamicLogManager instance with the provided dependencies.
// It initializes internal state and sets up all required components for managing
// dynamic log subscriptions.
//
// Parameters:
//   - nc: NATS connection for subscribing to job status events
//   - logConsumer: HadesLogConsumer for reading job logs
//   - aggregator: LogAggregator for storing and processing logs
//   - cfg: watcher lifetime bounds; a non-positive MaxLifetime uses the default
//
// Returns:
//   - LogManager: A new instance ready to start listening for job status changes
func NewDynamicLogManager(nc *nats.Conn, logConsumer logs.LogConsumer, aggregator watchedAggregator, cfg LogWatcherConfig) logs.LogManager {
	if cfg.MaxLifetime <= 0 {
		cfg.MaxLifetime = defaultWatcherMaxLifetime
	}
	return &DynamicLogManager{
		nc:            nc,
		logConsumer:   logConsumer,
		logAggregator: aggregator,
		maxLifetime:   cfg.MaxLifetime,
		watchers:      make(map[string]watcherState),
		finalized:     make(map[string]time.Time),
	}
}

// StartListening begins listening for job status changes on NATS subjects and manages
// log watching accordingly. It subscribes to the job lifecycle status events
// (subjects are formatted via buildstatus.StatusSubject, e.g. "hades.jobstatus.Running"):
//   - Queued: records the job as visible before it runs (no log watching yet)
//   - Running: starts log watching for the job
//   - Succeeded / Failed / Stopped: stops log watching for the job
//
// The method expects job IDs to be sent as string data in NATS messages.
//
// Parameters:
//   - ctx: Context for managing the lifecycle of subscriptions
//
// Returns:
//   - error: Any error that occurred while setting up NATS subscriptions
func (dlm *DynamicLogManager) StartListening(ctx context.Context) error {
	// One wildcard subscription rather than one per status: callbacks of a single
	// subscription run in arrival order, while separate subscriptions each get
	// their own goroutine. With one per status a fast job's Succeeded could be
	// handled before its Running, leaving a watcher that nothing ever stops.
	sub, err := dlm.nc.Subscribe(buildstatus.StatusSubject("*"), func(msg *nats.Msg) {
		jobID, err := dlm.extractJobID(msg)
		if err != nil {
			slog.Warn("Invalid message received",
				"subject", msg.Subject,
				"error", err)
			return
		}
		dlm.handleStatus(ctx, buildstatus.StatusFromSubject(msg.Subject), jobID)
	})
	if err != nil {
		return err
	}
	go dlm.sweepFinalizedLoop(ctx)

	// Block until the context is cancelled, then clean up inline. Because the
	// caller runs StartListening inside the application's shutdown WaitGroup,
	// returning only after cleanup means the process actually waits for the
	// drain + in-flight log forwarding to finish instead of racing exit.
	<-ctx.Done()
	slog.Info("Shutting down log manager subscriptions")
	if err := sub.Drain(); err != nil {
		slog.Warn("Failed to drain subscription", "error", err)
	}
	// Drain is asynchronous: until the subscription is closed a status callback may
	// still call sendWG.Add, which must not race with the Wait below. The stopping
	// flag covers the callbacks that outlive the close timeout and the watcher
	// lifetime timers, which are not tied to the subscription at all.
	waitSubscriptionClosed(sub, subscriptionCloseTimeout)
	dlm.mu.Lock()
	dlm.stopping = true
	dlm.mu.Unlock()
	// Wait for in-flight log-forwarding goroutines to finish (or cancel).
	dlm.sendWG.Wait()

	return nil
}

// finalizedSweepInterval is how often old entries leave the finalized set.
const finalizedSweepInterval = 10 * time.Minute

// sweepFinalizedLoop forgets finalized jobs after staleJobTTL until ctx ends. By
// then any duplicate terminal event is long past.
func (dlm *DynamicLogManager) sweepFinalizedLoop(ctx context.Context) {
	ticker := time.NewTicker(finalizedSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			cutoff := now.Add(-staleJobTTL)
			dlm.mu.Lock()
			for id, at := range dlm.finalized {
				if at.Before(cutoff) {
					delete(dlm.finalized, id)
				}
			}
			dlm.mu.Unlock()
		}
	}
}

// handleStatus dispatches one status event to its handler.
func (dlm *DynamicLogManager) handleStatus(ctx context.Context, status buildstatus.JobStatus, jobID string) {
	switch status {
	case buildstatus.StatusQueued:
		dlm.handleJobQueued(ctx, jobID)
	case buildstatus.StatusRunning:
		dlm.handleJobRunning(ctx, jobID)
	case buildstatus.StatusSucceeded:
		dlm.handleJobSucceeded(ctx, jobID)
	case buildstatus.StatusFailed:
		dlm.handleJobFailed(ctx, jobID)
	case buildstatus.StatusStopped:
		dlm.handleJobStopped(ctx, jobID)
	default:
		slog.Warn("Ignoring unknown job status", "status", status, "job_id", jobID)
	}
}

// extractJobID extracts and validates job ID from NATS message
func (dlm *DynamicLogManager) extractJobID(msg *nats.Msg) (string, error) {
	if len(msg.Data) == 0 {
		return "", ErrEmptyJobID
	}
	return string(msg.Data), nil
}

// handleJobQueued records a job as queued. It does not watch logs yet - no
// container exists until the job starts running - it only makes the job visible
// via GET /jobs so the dashboard can show queued work.
func (dlm *DynamicLogManager) handleJobQueued(_ context.Context, jobID string) {
	slog.Info("Job queued", "job_id", jobID)
	dlm.logAggregator.UpdateJobStatus(jobID, buildstatus.StatusQueued)
}

// handleJobStopped handles the terminal job stopped status event, treating it
// like a failure for log-watching purposes.
func (dlm *DynamicLogManager) handleJobStopped(ctx context.Context, jobID string) {
	slog.Info("Job stopped", "job_id", jobID)
	dlm.logAggregator.UpdateJobStatus(jobID, buildstatus.StatusStopped)
	dlm.stopWatchingJobLogs(ctx, jobID)
}

// handleJobRunning handles the job running status event
func (dlm *DynamicLogManager) handleJobRunning(ctx context.Context, jobID string) {
	slog.Info("Job started running", "job_id", jobID)
	dlm.logAggregator.UpdateJobStatus(jobID, buildstatus.StatusRunning)
	dlm.startWatchingJobLogs(ctx, jobID)
}

// handleJobSucceeded handles job succeeded status event
func (dlm *DynamicLogManager) handleJobSucceeded(ctx context.Context, jobID string) {
	slog.Info("Job succeeded", "job_id", jobID)
	dlm.logAggregator.UpdateJobStatus(jobID, buildstatus.StatusSucceeded)
	dlm.stopWatchingJobLogs(ctx, jobID)
}

// handleJobFailed handles job failed status event
func (dlm *DynamicLogManager) handleJobFailed(ctx context.Context, jobID string) {
	slog.Info("Job failed", "job_id", jobID)
	dlm.logAggregator.UpdateJobStatus(jobID, buildstatus.StatusFailed)
	dlm.stopWatchingJobLogs(ctx, jobID)
}

// subscriptionCloseTimeout bounds how long shutdown waits for drained subscriptions
// to deliver their remaining callbacks.
const subscriptionCloseTimeout = 5 * time.Second

// waitSubscriptionClosed waits until the drained subscription has closed, or the
// timeout runs out. StatusChanged fires immediately for a subscription that is
// already closed, so calling this after Drain can't miss it.
func waitSubscriptionClosed(sub *nats.Subscription, timeout time.Duration) {
	select {
	case <-sub.StatusChanged(nats.SubscriptionClosed):
	case <-time.After(timeout):
		slog.Warn("Timed out waiting for drained subscription to close")
	}
}

// startWatchingJobLogs initiates log watching for a specific job according to jobID.
// If a watcher already exists, it cancels the existing one before starting a new one.
// The method runs the log watching in a separate goroutine to avoid blocking.
//
// Parameters:
//   - ctx: Parent context for creating the job-specific context
//   - jobID: Unique identifier for the job to watch logs for
func (dlm *DynamicLogManager) startWatchingJobLogs(ctx context.Context, jobID string) {
	// Create new context for this job outside the lock
	jobCtx, cancel := context.WithCancel(ctx)
	drain := make(chan struct{})
	done := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Minimize critical section - only lock for map operations
	dlm.mu.Lock()
	// A Running for a job that was already forwarded is a new run (e.g. a
	// redelivery), whose outcome must be forwarded again.
	delete(dlm.finalized, jobID)
	oldWatcher, exists := dlm.watchers[jobID]
	dlm.watchers[jobID] = watcherState{
		ctx:    jobCtx,
		cancel: cancel,
		drain:  drain,
		wg:     wg,
	}
	dlm.mu.Unlock()

	// Start watching logs for this job
	go func() {
		defer wg.Done()
		defer close(done)
		defer func() {
			// Use a more efficient cleanup check
			dlm.mu.Lock()
			if watcher, ok := dlm.watchers[jobID]; ok && watcher.ctx == jobCtx {
				delete(dlm.watchers, jobID)
			}
			dlm.mu.Unlock()
		}()

		// Retire a replaced watcher here rather than in the caller: the caller is
		// the status subscription, and waiting there would hold up every other
		// job's status events until the old watcher has wound down.
		if exists {
			oldWatcher.cancel()
			oldWatcher.wg.Wait()
			dlm.resetJobLogs(jobID)
		}

		slog.Info("Starting to watch job logs", "job_id", jobID)
		err := dlm.logConsumer.WatchJobLogs(jobCtx, jobID, drain, func(batchedLog logs.Log) {
			dlm.logAggregator.AddLog(batchedLog)

			slog.Debug("Received batched job logs",
				"job_id", batchedLog.JobID,
				"container_id", batchedLog.ContainerID,
				"log_count", len(batchedLog.Logs))
		})

		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Error watching job logs",
				"job_id", jobID,
				"error", err)
		}
	}()

	go dlm.enforceWatcherLifetime(jobCtx, jobID, drain, done)
}

// enforceWatcherLifetime cancels the watcher once it outlives the job's own
// timeout (plus slack), or maxLifetime when the job has none. It also resolves
// the job's log target up front, so the callback URL is already cached when the
// job finishes and the forward does not wait on KV.
func (dlm *DynamicLogManager) enforceWatcherLifetime(jobCtx context.Context, jobID string, drain chan struct{}, done <-chan struct{}) {
	start := time.Now()
	var timeoutSeconds int64
	if target, ok := dlm.logAggregator.PrefetchJob(jobCtx, jobID); ok {
		timeoutSeconds = target.TimeoutSeconds
	}
	lifetime := watcherLifetime(timeoutSeconds, dlm.maxLifetime)

	timer := time.NewTimer(lifetime - time.Since(start))
	defer timer.Stop()
	select {
	case <-timer.C:
		dlm.expireWatcher(jobID, drain, lifetime)
	case <-done:
	case <-jobCtx.Done():
	}
}

// watcherLifetime returns how long a job's watcher may live: the job's own
// timeout plus slack when it has one, otherwise the configured ceiling.
func watcherLifetime(timeoutSeconds int64, ceiling time.Duration) time.Duration {
	if timeoutSeconds <= 0 {
		return ceiling
	}
	// Anything past the job retention is moot and would risk Duration overflow.
	if timeoutSeconds > int64(staleJobTTL/time.Second) {
		return staleJobTTL
	}
	return time.Duration(timeoutSeconds)*time.Second + watcherLifetimeSlack
}

// expireWatcher cancels the watcher owning drain without completing the job.
// The job may simply be running longer than expected (a job without a timeout
// is not bounded by anything), so forwarding now would hand the callback a
// truncated log and mark a running job completed. Its terminal status, when it
// does arrive, replays the full log from JetStream instead.
func (dlm *DynamicLogManager) expireWatcher(jobID string, drain chan struct{}, lifetime time.Duration) {
	dlm.mu.Lock()
	watcher, ok := dlm.watchers[jobID]
	if !ok || watcher.drain != drain || watcher.expired {
		dlm.mu.Unlock()
		return
	}
	watcher.expired = true
	dlm.watchers[jobID] = watcher
	dlm.mu.Unlock()

	slog.Warn("Log watcher exceeded its lifetime without a terminal status; stopping it",
		"job_id", jobID, "lifetime", lifetime)
	watcher.cancel()
}

// stopWatchingJobLogs handles a job's terminal status: it stops log watching for
// the job, then marks it completed and forwards its logs. A duplicate terminal
// status for an already handled job is a no-op.
//
// With a live watcher it performs a graceful drain: it closes the watcher's drain
// channel and waits for the watcher goroutine to finish, which only happens once the
// consumer has caught up to the stream (or the drain timeout elapses). Because job logs
// are published to JetStream before the terminal status is announced over core NATS,
// this guarantees every batch has been aggregated into the in-memory store before the
// job is marked completed and the logs are forwarded. Both the dashboard (GetJobLogs)
// and the forward (SendJobLogs) read that same in-memory store, so draining first keeps
// both complete.
//
// Without a live watcher (its Running event was missed, it expired, or this process
// started mid-job) it replays the job's whole log from JetStream before forwarding.
//
// This method is thread-safe and can be called concurrently from multiple goroutines.
//
// Parameters:
//   - ctx: Context bounding the asynchronous log-forwarding request
//   - jobID: Unique identifier of the job to stop watching logs for
func (dlm *DynamicLogManager) stopWatchingJobLogs(ctx context.Context, jobID string) {
	dlm.mu.Lock()
	if dlm.stopping {
		dlm.mu.Unlock()
		return
	}
	if _, done := dlm.finalized[jobID]; done {
		dlm.mu.Unlock()
		return
	}
	dlm.finalized[jobID] = time.Now()
	watcher, exists := dlm.watchers[jobID]
	if exists {
		delete(dlm.watchers, jobID)
	}
	// Add under the lock that guards stopping, so shutdown's Wait never races it.
	// It happens before the goroutine starts so that Wait sees it.
	dlm.sendWG.Add(1)
	dlm.mu.Unlock()

	replay := !exists || watcher.expired
	if replay {
		slog.Info("Terminal status without a live log watcher; replaying logs", "job_id", jobID)
	} else {
		slog.Info("Stopping log watch", "job_id", jobID)
		// Request a graceful drain. Do NOT hard-cancel first: cancelling would abandon
		// the still-in-flight JetStream batches that the fast core-NATS status raced
		// ahead of.
		close(watcher.drain)
	}

	// Don't wait for the drain here: all callbacks of a subscription share one
	// goroutine, so a slow drain would delay every other job's terminal status.
	go func() {
		defer dlm.sendWG.Done()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("Panic while sending job logs", "job_id", jobID, "panic", r)
			}
		}()

		if exists {
			// For an expired watcher this waits out its final flush, so nothing it
			// still adds can land on top of the replay.
			watcher.wg.Wait()
			// Releasing the job context now just frees its resources (and satisfies
			// the lostcancel vet check).
			watcher.cancel()
		}
		if replay {
			dlm.replayJobLogs(ctx, jobID)
		}

		// Only forward logs once every batch is aggregated and the job is completed.
		dlm.logAggregator.MarkJobCompleted(jobID)
		if replay && len(dlm.logAggregator.GetJobLogs(jobID)) == 0 {
			// Every container that starts publishes at least an empty slot, so no
			// logs means the job never ran (e.g. it failed in the queue). There is
			// nothing to forward.
			slog.Debug("No logs for job, skipping log forwarding", "job_id", jobID)
			return
		}
		if err := dlm.logAggregator.SendJobLogs(ctx, jobID); err != nil {
			slog.Error("Failed to send job logs", "job_id", jobID, "error", err)
		}
	}()
}

// replayJobLogs re-reads the job's whole log from JetStream into the aggregator,
// replacing whatever a previous watcher had collected.
func (dlm *DynamicLogManager) replayJobLogs(ctx context.Context, jobID string) {
	if dlm.logConsumer == nil {
		return
	}
	dlm.resetJobLogs(jobID)
	// An already-closed drain makes the watch read up to the subject's last
	// stored message and return.
	drain := make(chan struct{})
	close(drain)
	if err := dlm.logConsumer.WatchJobLogs(ctx, jobID, drain, dlm.logAggregator.AddLog); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("Failed to replay job logs", "job_id", jobID, "error", err)
	}
}

// resetJobLogs drops the job's aggregated logs ahead of a watcher that will read
// them all again.
func (dlm *DynamicLogManager) resetJobLogs(jobID string) {
	dlm.logAggregator.ResetJobLogs(jobID)
}
