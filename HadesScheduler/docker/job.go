package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"

	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/hades-scheduler/hades/shared/timing"
	"github.com/moby/moby/client"
)

// Job is a single queued job bound to the Docker client and options used to run
// it. Its steps execute in order and share the job's volume; see the execute
// method for the ContinueOnError semantics.
type Job struct {
	cli    *client.Client
	logger *slog.Logger
	Options
	payload.QueuePayload
	publisher buildlogs.LogPublisher
	timer     *timing.JobTimer
}

type jobIDContextKey string

func (d Job) execute(ctx context.Context) error {
	// Failures from steps marked ContinueOnError are tolerated by design (e.g. a test step whose
	// failure is a normal, expected outcome that a later step, such as result parsing, still needs
	// to report). They're joined here purely for logging and must not fail the overall job - that
	// would defeat the point of ContinueOnError and would be inconsistent with the Kubernetes
	// executor, where such a step never fails the job at all.
	stepErr := error(nil)

	for _, step := range d.Steps {
		d.logger.Info("Executing step", slog.Any("step", step))

		// Copy the global envs and add the step specific ones
		var envs = make(map[string]string)
		maps.Copy(envs, d.Metadata)
		maps.Copy(envs, step.Metadata)
		envs["UUID"] = d.ID.String()
		envs["JOB_NAME"] = d.Name
		step.Metadata = envs

		dockerStep := Step{
			cli:       d.cli,
			logger:    d.logger,
			Options:   d.Options,
			Step:      step,
			publisher: d.publisher,
			timer:     d.timer,
		}

		stepCtx := context.WithValue(ctx, jobIDContextKey("job_id"), d.ID.String())
		err := dockerStep.execute(stepCtx)
		if err != nil {
			d.logger.Error("Failed to execute step", slog.Any("error", err))
			// A cancelled/expired job context (e.g. the whole-job timeout) must
			// abort the job immediately, even for ContinueOnError steps - otherwise
			// a timeout would be swallowed and later steps would keep running.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if step.ContinueOnError {
				d.logger.Info("Next step should be executed despite error due to ContinueOnError setting", slog.Any("step", step))
				stepErr = errors.Join(stepErr, fmt.Errorf("step %v failed with ContinueOnError set: %w", step.ID, err))
				continue
			}
			return err
		}
	}

	if stepErr != nil {
		d.logger.Warn("Job completed with tolerated step failures", slog.Any("error", stepErr))
	}
	return nil
}
