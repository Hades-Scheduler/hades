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
	logConsumer   *logs.HadesLogConsumer
	logAggregator logs.LogAggregator
	mu            sync.RWMutex
	watchers      map[string]watcherState // jobID -> watcher state
	sendWG        sync.WaitGroup          // tracks in-flight log-forwarding goroutines
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
}

// NewDynamicLogManager creates a new DynamicLogManager instance with the provided dependencies.
// It initializes internal state and sets up all required components for managing
// dynamic log subscriptions.
//
// Parameters:
//   - nc: NATS connection for subscribing to job status events
//   - logConsumer: HadesLogConsumer for reading job logs
//   - aggregator: LogAggregator for storing and processing logs
//
// Returns:
//   - LogManager: A new instance ready to start listening for job status changes
func NewDynamicLogManager(nc *nats.Conn, logConsumer *logs.HadesLogConsumer, aggregator logs.LogAggregator) logs.LogManager {
	return &DynamicLogManager{
		nc:            nc,
		logConsumer:   logConsumer,
		logAggregator: aggregator,
		watchers:      make(map[string]watcherState),
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
	subs := make([]*nats.Subscription, 0, 5)

	// Subscribe to queued status - record the job so it is visible in GET /jobs
	// before it starts running. Nothing watches logs yet at this point.
	sub, err := dlm.subscribeToStatus(ctx, buildstatus.StatusQueued, dlm.handleJobQueued)
	if err != nil {
		return err
	}
	subs = append(subs, sub)

	// Subscribe to running status - start watching logs
	sub, err = dlm.subscribeToStatus(ctx, buildstatus.StatusRunning, dlm.handleJobRunning)
	if err != nil {
		dlm.cleanupSubscriptions(subs)
		return err
	}
	subs = append(subs, sub)

	// Subscribe to succeeded status - stop watching logs
	sub, err = dlm.subscribeToStatus(ctx, buildstatus.StatusSucceeded, dlm.handleJobSucceeded)
	if err != nil {
		dlm.cleanupSubscriptions(subs)
		return err
	}
	subs = append(subs, sub)

	// Subscribe to failed status - stop watching logs
	sub, err = dlm.subscribeToStatus(ctx, buildstatus.StatusFailed, dlm.handleJobFailed)
	if err != nil {
		dlm.cleanupSubscriptions(subs)
		return err
	}
	subs = append(subs, sub)

	// Subscribe to stopped status - stop watching logs (terminal, like failed)
	sub, err = dlm.subscribeToStatus(ctx, buildstatus.StatusStopped, dlm.handleJobStopped)
	if err != nil {
		dlm.cleanupSubscriptions(subs)
		return err
	}
	subs = append(subs, sub)

	// Block until the context is cancelled, then clean up inline. Because the
	// caller runs StartListening inside the application's shutdown WaitGroup,
	// returning only after cleanup means the process actually waits for the
	// drain + in-flight log forwarding to finish instead of racing exit.
	<-ctx.Done()
	slog.Info("Shutting down log manager subscriptions")
	dlm.cleanupSubscriptions(subs)
	// Drain is asynchronous: until the subscriptions are closed a status callback may
	// still call sendWG.Add, which must not race with the Wait below.
	waitSubscriptionsClosed(subs, subscriptionCloseTimeout)
	// Wait for in-flight log-forwarding goroutines to finish (or cancel).
	dlm.sendWG.Wait()

	return nil
}

// subscribeToStatus creates a subscription to a job status subject
func (dlm *DynamicLogManager) subscribeToStatus(ctx context.Context, jobStatus buildstatus.JobStatus, handler func(context.Context, string)) (*nats.Subscription, error) {
	return dlm.nc.Subscribe(buildstatus.StatusSubject(jobStatus), func(msg *nats.Msg) {
		jobID, err := dlm.extractJobID(msg)
		if err != nil {
			slog.Warn("Invalid message received",
				"subject", msg.Subject,
				"error", err)
			return
		}
		handler(ctx, jobID)
	})
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

// waitSubscriptionsClosed waits until all drained subscriptions have closed, or the
// timeout runs out. StatusChanged fires immediately for a subscription that is
// already closed, so calling this after Drain can't miss it.
func waitSubscriptionsClosed(subs []*nats.Subscription, timeout time.Duration) {
	deadline := time.After(timeout)
	for _, sub := range subs {
		select {
		case <-sub.StatusChanged(nats.SubscriptionClosed):
		case <-deadline:
			slog.Warn("Timed out waiting for drained subscriptions to close")
			return
		}
	}
}

// cleanupSubscriptions drains all subscriptions
func (dlm *DynamicLogManager) cleanupSubscriptions(subs []*nats.Subscription) {
	for _, sub := range subs {
		if err := sub.Drain(); err != nil {
			slog.Warn("Failed to drain subscription", "error", err)
		}
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
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Minimize critical section - only lock for map operations
	dlm.mu.Lock()
	oldWatcher, exists := dlm.watchers[jobID]
	dlm.watchers[jobID] = watcherState{
		ctx:    jobCtx,
		cancel: cancel,
		drain:  drain,
		wg:     wg,
	}
	dlm.mu.Unlock()

	// Cancel old watcher outside the lock to avoid potential deadlock
	if exists {
		oldWatcher.cancel()
		oldWatcher.wg.Wait()
	}

	// Start watching logs for this job
	go func() {
		defer wg.Done()
		defer func() {
			// Use a more efficient cleanup check
			dlm.mu.Lock()
			if watcher, ok := dlm.watchers[jobID]; ok && watcher.ctx == jobCtx {
				delete(dlm.watchers, jobID)
			}
			dlm.mu.Unlock()
		}()

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
}

// stopWatchingJobLogs stops log watching for a specific job and removes it from the
// watchers map. If no watcher exists for the given job ID, the method returns without
// error.
//
// It performs a graceful drain: it closes the watcher's drain channel and waits for the
// watcher goroutine to finish, which only happens once the consumer has caught up to the
// stream (or the drain timeout elapses). Because job logs are published to JetStream
// before the terminal status is announced over core NATS, this guarantees every batch
// has been aggregated into the in-memory store before the job is marked completed and the
// logs are forwarded. Both the dashboard (GetJobLogs) and the forward (SendJobLogs) read
// that same in-memory store, so draining first keeps both complete.
//
// This method is thread-safe and can be called concurrently from multiple goroutines.
//
// Parameters:
//   - ctx: Context bounding the asynchronous log-forwarding request
//   - jobID: Unique identifier of the job to stop watching logs for
func (dlm *DynamicLogManager) stopWatchingJobLogs(ctx context.Context, jobID string) {
	dlm.mu.Lock()
	watcher, exists := dlm.watchers[jobID]
	if exists {
		delete(dlm.watchers, jobID)
	}
	dlm.mu.Unlock()

	if !exists {
		return
	}

	slog.Info("Stopping log watch", "job_id", jobID)
	// Request a graceful drain. Do NOT hard-cancel first: cancelling would abandon the
	// still-in-flight JetStream batches that the fast core-NATS status raced ahead of.
	close(watcher.drain)

	// Don't wait for the drain here: all callbacks of a subscription share one
	// goroutine, so a slow drain would delay every other job's terminal status.
	// sendWG.Add happens before the goroutine starts so shutdown's Wait sees it.
	dlm.sendWG.Add(1)
	go func() {
		defer dlm.sendWG.Done()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("Panic while sending job logs", "job_id", jobID, "panic", r)
			}
		}()

		watcher.wg.Wait()
		// The goroutine has finished draining; releasing the job context now just frees
		// its resources (and satisfies the lostcancel vet check).
		watcher.cancel()

		// Only forward logs once every batch is aggregated and the job is completed.
		dlm.logAggregator.MarkJobCompleted(jobID)
		if err := dlm.logAggregator.SendJobLogs(ctx, jobID); err != nil {
			slog.Error("Failed to send job logs", "job_id", jobID, "error", err)
		}
	}()
}
