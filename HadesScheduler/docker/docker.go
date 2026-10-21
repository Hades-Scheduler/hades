package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/hades-scheduler/hades/hadesScheduler/log"
	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/payload"
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

// volumeInUseRetryWindow caps how long volume removal keeps retrying while the
// daemon still reports the volume as attached. Containers are removed before
// teardown, but an auto-removing one can take a moment to fully detach.
const volumeInUseRetryWindow = 5 * time.Second

func deleteSharedVolume(ctx context.Context, cli *client.Client, name string) error {
	err := removeVolumeWithRetry(ctx, func(ctx context.Context) error {
		_, err := cli.VolumeRemove(ctx, name, client.VolumeRemoveOptions{Force: true})
		return err
	}, 25*time.Millisecond, volumeInUseRetryWindow)
	if err != nil {
		slog.Error("Failed to delete shared volume", slog.Any("error", err))
		return err
	}

	slog.Debug("Volume deleted", slog.Any("volume", name))

	return nil
}

// isVolumeInUse reports whether err is the daemon refusing to remove a volume
// that a container still references. Older daemons return a plain 500 with only
// the message, so the text is checked as well as the conflict status.
func isVolumeInUse(err error) bool {
	return cerrdefs.IsConflict(err) || strings.Contains(err.Error(), "volume is in use")
}

// removeVolumeWithRetry retries remove with a doubling backoff while the volume is
// still in use, until the window runs out. Any other error is returned right away.
// The first attempt isn't delayed, so the usual case costs a single call.
func removeVolumeWithRetry(ctx context.Context, remove func(context.Context) error, initialDelay, window time.Duration) error {
	deadline := time.Now().Add(window)
	delay := initialDelay
	for {
		err := remove(ctx)
		if err == nil || !isVolumeInUse(err) {
			return err
		}
		if time.Now().Add(delay).After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(delay):
		}
		delay *= 2
	}
}

// imageRef is one distinct image of a job plus the first step that uses it. The
// pull timing is recorded against that step.
type imageRef struct {
	image string
	step  int
}

// distinctImages returns each image of steps once, in first-use order.
func distinctImages(steps []payload.Step) []imageRef {
	seen := make(map[string]struct{}, len(steps))
	var refs []imageRef
	for _, s := range steps {
		if _, ok := seen[s.Image]; ok {
			continue
		}
		seen[s.Image] = struct{}{}
		refs = append(refs, imageRef{image: s.Image, step: s.ID})
	}
	return refs
}

// imagePuller wraps the daemon calls of a prefetch so tests can run it without a
// daemon.
type imagePuller struct {
	policy  PullPolicy
	present func(ctx context.Context, image string) bool
	pull    func(ctx context.Context, image string) error
	record  func(step int, start, end time.Time, cached bool)
}

// pendingImage is the result of one image's prefetch. err is set once done closes.
type pendingImage struct {
	done chan struct{}
	err  error
}

// wait blocks until the image is resolved and returns the pull outcome.
func (pi *pendingImage) wait() error {
	<-pi.done
	return pi.err
}

// prefetch starts resolving all refs in parallel and returns one handle per image,
// so a step only waits for its own image and not the slowest one. Errors go through
// the handle instead of failing the job, so they still surface at the step that
// needs the image and ContinueOnError keeps working.
func (p imagePuller) prefetch(ctx context.Context, refs []imageRef) map[string]*pendingImage {
	out := make(map[string]*pendingImage, len(refs))
	for _, ref := range refs {
		pi := &pendingImage{done: make(chan struct{})}
		out[ref.image] = pi
		go func() {
			defer close(pi.done)
			// Check presence before starting the clock so the local lookup
			// isn't counted as pull time.
			cached := p.present(ctx, ref.image)
			start := time.Now()
			switch {
			case p.policy.shouldPull(cached):
				pi.err = p.pull(ctx, ref.image)
			case !cached:
				// Only reachable under PullNever. Fail here rather than letting
				// ContainerCreate report a confusing "No such image" later.
				pi.err = fmt.Errorf(
					"image %s is not present locally and the pull policy is %q",
					ref.image, p.policy)
			}
			// Record even when the pull is skipped. A missing phase breaks the
			// per-job overhead rollup, and a near-zero image_pull with
			// cached=true shows the policy is working.
			p.record(ref.step, start, time.Now(), cached)
		}()
	}
	return out
}
