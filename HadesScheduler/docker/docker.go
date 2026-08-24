package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/hades-scheduler/hades/hadesScheduler/log"
	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"
	"github.com/moby/moby/client/pkg/jsonmessage"
)

// streamContainerLogs follows a container's logs and publishes them
// incrementally as they are produced, rather than buffering to EOF and
// publishing once after the container stops. It blocks until the follow stream
// closes (the container stopped) or ctx is cancelled.
//
// StdCopy demultiplexes the interleaved Docker stream into separate stdout and
// stderr pipes so each can be scanned live. The pipe writers are always closed
// (via CloseWithError) when StdCopy returns, so the scanners never block waiting
// for EOF, even on error.
func streamContainerLogs(ctx context.Context, cli *client.Client, publisher buildlogs.LogPublisher, containerID, jobID string) error {
	logReader, err := cli.ContainerLogs(ctx, containerID, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Timestamps: true,
		Follow:     true,
	})
	if err != nil {
		return fmt.Errorf("getting container logs: %w", err)
	}
	defer logReader.Close()

	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()

	go func() {
		_, copyErr := stdcopy.StdCopy(stdoutW, stderrW, logReader)
		// Closing with the (possibly nil) error unblocks the scanners with EOF on
		// success or the error otherwise.
		_ = stdoutW.CloseWithError(copyErr)
		_ = stderrW.CloseWithError(copyErr)
	}()

	return log.StreamContainerLogs(ctx, publisher, jobID, containerID, log.StreamOptions{RegisterSlot: true},
		log.StreamSource{Reader: stdoutR, StreamType: log.StreamStdout},
		log.StreamSource{Reader: stderrR, StreamType: log.StreamStderr},
	)
}

func removeContainer(ctx context.Context, cli *client.Client, containerID string) error {
	if _, err := cli.ContainerRemove(ctx, containerID, client.ContainerRemoveOptions{
		Force:         true, // Kill if running, then remove
		RemoveVolumes: true, // Clean up any volumes
	}); err != nil {
		// The container may already be gone (e.g. AutoRemove reaped it, or it was
		// removed on a prior cleanup): treat that as success rather than a noisy error.
		if cerrdefs.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to cleanup container %s: %w", containerID, err)
	}

	slog.Info("Container cleanup done", slog.String("container_id", containerID))
	return nil
}

// imagePresentLocally reports whether the image is already in the local Docker
// cache, so image_pull timing can be split into cold and warm pulls. A failed
// inspect (image absent, or any error) is treated as not-present; this is a
// best-effort label, not a correctness gate.
func imagePresentLocally(ctx context.Context, cli *client.Client, image string) bool {
	_, err := cli.ImageInspect(ctx, image)
	return err == nil
}

func pullImages(ctx context.Context, cli *client.Client, images ...string) error {
	var wg sync.WaitGroup
	errorsCh := make(chan error, len(images))

	for _, image := range images {
		wg.Add(1)

		go func(img string) {
			defer wg.Done()

			response, err := cli.ImagePull(ctx, img, client.ImagePullOptions{})
			if err != nil {
				errorsCh <- fmt.Errorf("failed to pull image %s: %w", img, err)
				return
			}
			defer response.Close()
			// Decode the pull response stream to completion so that errors the
			// daemon reports in-band (auth failures, missing manifests, ...) are
			// surfaced - not just transport errors returned by ImagePull itself.
			// A plain io.Copy would drain the bytes but ignore those JSON records.
			if err := jsonmessage.DisplayJSONMessagesStream(response, io.Discard, 0, false, nil); err != nil {
				errorsCh <- fmt.Errorf("failed to pull image %s: %w", img, err)
			}
		}(image)
	}

	// wait for all goroutines to complete
	wg.Wait()
	close(errorsCh)

	// Collect errors, keeping them wrapped so callers can still use errors.Is/As.
	var pullErrs []error
	for err := range errorsCh {
		pullErrs = append(pullErrs, err)
	}

	if len(pullErrs) > 0 {
		return fmt.Errorf("encountered %d errors while pulling images: %w", len(pullErrs), errors.Join(pullErrs...))
	}

	return nil
}

func createSharedVolume(ctx context.Context, cli *client.Client, name string) error {
	// Create the volume
	_, err := cli.VolumeCreate(ctx, client.VolumeCreateOptions{
		Name: name,
	})
	if err != nil {
		slog.Error("Failed to create shared volume", slog.Any("error", err))
		return err
	}

	slog.Debug("Volume created", slog.Any("volume", name))
	return nil
}

func deleteSharedVolume(ctx context.Context, cli *client.Client, name string) error {
	// Delete the volume
	_, err := cli.VolumeRemove(ctx, name, client.VolumeRemoveOptions{Force: true})
	if err != nil {
		slog.Error("Failed to delete shared volume", slog.Any("error", err))
		return err
	}

	slog.Debug("Volume deleted", slog.Any("volume", name))

	return nil
}
