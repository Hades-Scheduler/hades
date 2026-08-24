// Package docker implements the Docker executor: it runs each step of a job as
// a container on the local Docker daemon. All steps of a job share a per-job
// named volume ("shared-<jobID>") that is created before the first step and
// removed after the last, so files written by one step are visible to later
// ones. This executor targets local development; production uses the Kubernetes
// executor and operator.
package docker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hades-scheduler/hades/hadesScheduler/log"
	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/hades-scheduler/hades/shared/redact"
	"github.com/hades-scheduler/hades/shared/timing"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// Options holds the per-scheduler Docker execution settings applied to every
// step container (script shell, resource limits, autoremove, and the per-job
// shared volume). Fields are set through the With* DockerOption builders.
type Options struct {
	scriptExecutor       string
	containerAutoremove  bool
	cpuLimit             uint
	memoryLimit          string
	volumeName           string
	containerLogsOptions container.LogConfig
}

// Scheduler runs jobs on the local Docker daemon and publishes their logs and
// status transitions via the configured publishers.
type Scheduler struct {
	Options
	cli             *client.Client
	logPublisher    buildlogs.LogPublisher
	statusPublisher buildstatus.StatusPublisher
}

// NewScheduler builds a Scheduler from the default configuration and applies the
// given options in order. It returns an error if the Docker client cannot be
// created or any option fails.
func NewScheduler(options ...DockerOption) (*Scheduler, error) {
	scheduler, err := NewDefaultScheduler()
	if err != nil {
		return nil, err
	}

	for _, option := range options {
		if err := option(scheduler); err != nil {
			return nil, err
		}
	}

	return scheduler, nil
}

// NewDefaultScheduler creates a Scheduler connected to the local Docker daemon
// with default options and no-op log/status publishers (override them with the
// With* options).
func NewDefaultScheduler() (*Scheduler, error) {
	// Create a new Docker client
	cli, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		slog.Error("Failed to create Docker client", slog.Any("error", err))
		return nil, err
	}

	defaultOpts := Options{
		scriptExecutor:      "/bin/bash -c",
		containerAutoremove: false,
		cpuLimit:            0,
		memoryLimit:         "",
	}

	scheduler := &Scheduler{
		cli:             cli,
		Options:         defaultOpts,
		logPublisher:    log.NewNoopPublisher(), // Use no-op publisher by default
		statusPublisher: log.NewNoopPublisher(), // Use no-op publisher by default
	}

	return scheduler, nil
}

// ScheduleJob runs every step of job sequentially in its own container. It
// creates the per-job shared volume, publishes Running/Succeeded/Failed status
// transitions around execution, and removes the volume when done (even on
// cancellation). It returns the first step error that aborts the job.
func (d *Scheduler) ScheduleJob(ctx context.Context, job payload.QueuePayload) error {
	var jobLogger *slog.Logger
	var containerLogsOptions container.LogConfig

	jobLogger = slog.Default().With(slog.String("job_id", job.ID.String()))
	containerLogsOptions = container.LogConfig{}

	// Start the per-job timer, continuing the trace propagated from the API so
	// this job's phase spans nest under it. queue_wait spans the API submission
	// (job.Timestamp) to now; it crosses hosts, so a skewed clock is clamped to
	// zero by Record.
	traceCtx := ctx
	if job.TraceParent != "" {
		traceCtx = timing.Extract(ctx, map[string]string{"traceparent": job.TraceParent})
	}
	timer := timing.NewJobTimer(traceCtx, "docker", job.ID.String())
	if !job.Timestamp.IsZero() {
		timer.Record(0, timing.PhaseQueueWait, job.Timestamp, time.Now())
	}
	// Summary is registered first so it runs LAST (after the teardown defer
	// below), including teardown in the rollup.
	defer timer.Summary()

	// Create a unique volume name for this job
	volumeName := fmt.Sprintf("shared-%s", job.ID.String())
	// Create the shared volume
	provisionStart := time.Now()
	err := createSharedVolume(ctx, d.cli, volumeName)
	timer.Record(0, timing.PhaseProvision, provisionStart, time.Now())
	if err != nil {
		jobLogger.Error("Failed to create shared volume", slog.Any("error", err))
		return err
	}

	// Delete the shared volume after the job is done. Use a fresh context with
	// its own timeout so cleanup still runs even if the job context (ctx) has
	// already been cancelled, e.g. during a graceful shutdown - otherwise the
	// shared-<jobID> volume would leak.
	defer func() {
		teardownStart := time.Now()
		// Give any auto-removing containers a moment to detach from the volume.
		time.Sleep(500 * time.Millisecond)

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := deleteSharedVolume(cleanupCtx, d.cli, volumeName); err != nil {
			jobLogger.Error("Failed to delete shared volume", slog.Any("error", err))
			timer.Record(0, timing.PhaseTeardown, teardownStart, time.Now())
			return
		}

		jobLogger.Info("Volume deleted", slog.Any("volume", volumeName))
		timer.Record(0, timing.PhaseTeardown, teardownStart, time.Now())
	}()

	// Add created volume to the job's docker config
	jobDockerConfig := d.Options
	jobDockerConfig.volumeName = volumeName
	jobDockerConfig.containerLogsOptions = containerLogsOptions
	dockerJob := Job{
		cli:          d.cli,
		logger:       jobLogger,
		Options:      jobDockerConfig,
		QueuePayload: job,
		publisher:    d.logPublisher,
		timer:        timer,
	}

	//block to send status first before execution
	if err := d.statusPublisher.PublishJobStatus(ctx, buildstatus.StatusRunning, job.ID.String()); err != nil {
		jobLogger.Warn("failed to publish running status", "error", err)
	}

	// Apply the whole-job timeout, if configured. When it fires, the derived
	// context is cancelled, which unblocks the running step's ContainerWait and
	// triggers its force-cleanup so no container is left running.
	execCtx := ctx
	if job.TimeoutSeconds > 0 {
		// Clamp to the overflow-safe bound (the API rejects larger values, but a
		// payload may reach the scheduler from other producers): seconds *
		// time.Second must not overflow int64 nanoseconds into a negative duration.
		timeoutSeconds := job.TimeoutSeconds
		if timeoutSeconds > payload.MaxTimeoutSeconds {
			timeoutSeconds = payload.MaxTimeoutSeconds
		}
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
		defer cancel()
	}

	err = dockerJob.execute(execCtx)
	if err != nil {
		// Surface why the job failed (e.g. an image pull error) to the dashboard.
		// Redact secret-looking tokens and cap the length so a verbose daemon error
		// stays within NATS header limits.
		reason := redact.Default().Text(err.Error())
		// Distinguish a timeout from other failures for a clearer status message.
		// The parent ctx being live while execCtx is done means the job timed out
		// rather than the whole scheduler shutting down.
		if job.TimeoutSeconds > 0 && execCtx.Err() != nil && ctx.Err() == nil {
			reason = fmt.Sprintf("job timed out after %d seconds", job.TimeoutSeconds)
		}
		reason = buildstatus.TruncateReason(reason)
		// Publish with a fresh context: the job ctx may already be cancelled (on a
		// timeout), which would otherwise drop the failure status.
		if perr := d.statusPublisher.PublishJobStatus(context.WithoutCancel(ctx), buildstatus.StatusFailed, job.ID.String(), reason); perr != nil {
			jobLogger.Warn("failed to publish failed status", "error", perr)
		}
		jobLogger.Error("Failed to execute job", "error", err)
		return err
	}

	if err := d.statusPublisher.PublishJobStatus(ctx, buildstatus.StatusSucceeded, job.ID.String()); err != nil {
		jobLogger.Warn("failed to publish success status", "error", err)
	}
	jobLogger.Debug("Job executed successfully", "job_id", job.ID)

	return nil
}
