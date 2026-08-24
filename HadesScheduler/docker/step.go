package docker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/hades-scheduler/hades/shared/timing"
	"github.com/hades-scheduler/hades/shared/utils"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
)

// logStreamDrainTimeout bounds how long a step waits, after the container has
// stopped, for the log streamer to publish the container's final output. It
// guards against a wedged log stream hanging the step.
const logStreamDrainTimeout = 10 * time.Second

// Step is a single job step bound to the Docker client and options used to run
// it. Executing a Step pulls the image, creates a container mounting the shared
// volume at /shared, applies CPU/memory limits, runs the script, streams the
// container logs to the publisher, and reports a non-zero exit as an error.
type Step struct {
	cli    *client.Client
	logger *slog.Logger
	Options
	payload.Step
	publisher buildlogs.LogPublisher
	timer     *timing.JobTimer
}

func (s Step) execute(ctx context.Context) error {
	// Record whether the image is already present locally (a cache hit) so cold
	// and warm pulls are distinguishable. The probe is a local daemon call taken
	// before the pull clock starts, so it is not counted as image-pull time.
	cached := imagePresentLocally(ctx, s.cli, s.Image)
	pullStart := time.Now()
	err := pullImages(ctx, s.cli, s.Image)
	s.timer.RecordImagePull(s.ID, pullStart, time.Now(), cached)
	if err != nil {
		s.logger.Error("Failed to pull image", slog.Any("error", err))
		return err
	}

	var envs []string
	for k, v := range s.Metadata {
		envs = append(envs, fmt.Sprintf("%s=%s", k, v))
	}

	jobId, ok := ctx.Value(jobIDContextKey("job_id")).(string)
	if !ok {
		return fmt.Errorf("job_id not found in context")
	}

	// Add the job_id to the container envs
	envs = append(envs, fmt.Sprintf("UUID=%s", jobId))

	containerConfig := container.Config{
		Image:      s.Image,
		Env:        envs,
		WorkingDir: "/shared", // Set the working directory to the shared volume
		Labels:     map[string]string{"job_id": jobId},
	}

	hostConfig := container.HostConfig{
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeVolume,
				Source: s.volumeName,
				Target: "/shared",
			},
		},
		LogConfig:  s.containerLogsOptions,
		AutoRemove: s.Options.containerAutoremove, // Remove the container after it is done only if the config is set to true
	}

	// Limit the resource usage of the containers
	cpuLimit := utils.FindLimit(int(s.CPULimit), int(s.Options.cpuLimit))
	if cpuLimit != 0 {
		s.logger.Debug("Setting CPU limit to ", "limit", cpuLimit)
		hostConfig.Resources.NanoCPUs = int64(float64(cpuLimit) * 1e9)
	}
	ramLimit := utils.FindMemoryLimit(s.MemoryLimit, s.Options.memoryLimit)
	if ramLimit != 0 {
		s.logger.Debug("Setting RAM limit to ", "limit", ramLimit)
		hostConfig.Resources.Memory = ramLimit
	}

	// Total memory+swap limit. Validated at submission time to be >= the memory
	// limit and to require a memory limit, so it is safe to set directly here.
	if s.MemorySwap != "" {
		swapLimit, err := utils.ParseMemoryLimit(s.MemorySwap)
		if err != nil {
			return fmt.Errorf("parsing memory_swap: %w", err)
		}
		s.logger.Debug("Setting memory+swap limit to ", "limit", swapLimit)
		hostConfig.Resources.MemorySwap = swapLimit
	}

	if s.PidsLimit > 0 {
		s.logger.Debug("Setting PIDs limit to ", "limit", s.PidsLimit)
		hostConfig.Resources.PidsLimit = &s.PidsLimit
	}

	// Network mode (e.g. "none" to fully disable networking). Empty means the
	// Docker default. Validated at submission time.
	if s.Network != "" {
		s.logger.Debug("Setting network mode to ", "network", s.Network)
		hostConfig.NetworkMode = container.NetworkMode(s.Network)
	}

	// Create the bash script if there is one
	if s.Script != "" {
		// Overwrite the default entrypoint
		containerConfig.Entrypoint = strings.Split(s.scriptExecutor, " ")
		containerConfig.Entrypoint = append(containerConfig.Entrypoint, s.Script)
	}

	createStart := time.Now()
	resp, err := s.cli.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     &containerConfig,
		HostConfig: &hostConfig,
	})
	s.timer.Record(s.ID, timing.PhaseContainerCreate, createStart, time.Now())
	if err != nil {
		s.logger.Error("Failed to create container", slog.Any("error", err))
		return err
	}

	defer func() {
		// If the job context was cancelled (e.g. a timeout) the container may
		// still be running: AutoRemove only reaps containers that have already
		// exited, and the cancelled job ctx can no longer drive cleanup. Force
		// remove with a fresh context so the container is stopped and the shared
		// volume is released, regardless of the AutoRemove setting.
		if ctx.Err() != nil {
			removeStart := time.Now()
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if err := removeContainer(cleanupCtx, s.cli, resp.ID); err != nil {
				s.logger.Error("Failed to cleanup container after cancellation", slog.Any("error", err), slog.Any("container_id", resp.ID))
			}
			s.timer.Record(s.ID, timing.PhaseContainerRemove, removeStart, time.Now())
			return
		}
		if s.Options.containerAutoremove {
			return
		}
		removeStart := time.Now()
		if err := removeContainer(ctx, s.cli, resp.ID); err != nil {
			s.logger.Error("Failed to cleanup container", slog.Any("error", err), slog.Any("container_id", resp.ID))
		}
		s.timer.Record(s.ID, timing.PhaseContainerRemove, removeStart, time.Now())
	}()

	// Start the container
	startupStart := time.Now()
	_, err = s.cli.ContainerStart(ctx, resp.ID, client.ContainerStartOptions{})
	s.timer.Record(s.ID, timing.PhaseContainerStartup, startupStart, time.Now())
	if err != nil {
		s.logger.Error("Failed to start container", slog.Any("error", err))
		return err
	}
	// The container process now runs until it exits; ContainerWait below returns
	// when it stops. This monotonic window is the step's runtime.
	runStart := time.Now()

	// Follow the container's logs live, concurrently with the wait, so they are
	// published to NATS as they are produced rather than only after the container
	// stops. The follow stream closes on container exit (EOF), which completes the
	// streamer and its final flush.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		if err := streamContainerLogs(streamCtx, s.cli, s.publisher, resp.ID, jobId); err != nil {
			s.logger.Error("Failed to stream container logs to NATS", slog.Any("error", err), slog.Any("container_id", resp.ID))
		}
	}()

	// Wait for the container to finish
	waitResult := s.cli.ContainerWait(ctx, resp.ID, client.ContainerWaitOptions{
		Condition: container.WaitConditionNotRunning,
	})
	select {
	case err := <-waitResult.Error:
		s.timer.Record(s.ID, timing.PhaseContainerRun, runStart, time.Now())
		if err != nil {
			s.logger.Error("Error waiting for container", slog.Any("error", err), slog.Any("container_id", resp.ID))
			return err
		}
	case status := <-waitResult.Result:
		s.timer.Record(s.ID, timing.PhaseContainerRun, runStart, time.Now())
		// Wait (bounded) for the log streamer to drain and flush the container's
		// final output before checking status, so no tail lines are lost. A wedged
		// log stream cannot hang the step.
		drainStart := time.Now()
		select {
		case <-streamDone:
			s.logger.Debug("Container logs streamed to NATS", slog.Any("container_id", resp.ID), slog.Any("image", s.Image))
		case <-time.After(logStreamDrainTimeout):
			s.logger.Warn("Timed out waiting for container log stream to drain", slog.Any("container_id", resp.ID))
		}
		s.timer.Record(s.ID, timing.PhaseLogDrain, drainStart, time.Now())

		if status.StatusCode != 0 {
			s.logger.Error("Container exited with status", slog.Any("status", status.StatusCode), slog.Any("container_id", resp.ID), slog.Any("image", s.Image))
			return fmt.Errorf("container exited with status %d", status.StatusCode)
		}
	}

	s.logger.Debug("Container completed", slog.Any("container_id", resp.ID), slog.Any("image", s.Image))
	return nil
}
