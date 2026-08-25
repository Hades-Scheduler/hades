package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	"github.com/hades-scheduler/hades/hadesScheduler/k8s"
	"github.com/hades-scheduler/hades/shared/buildlogs"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const FinalizerContainerName = "buildjob-finalizer"

// DefaultFinalizerImage is the image for the pod's main container. Every
// BuildJob pod needs one, because Kubernetes requires at least one non-init
// container; the steps themselves run as init containers, so this one only has
// to exit 0 once they are done.
//
// Pinned to an explicit patch tag rather than left bare. An untagged image
// resolves to ":latest", which Kubernetes special-cases to
// imagePullPolicy: Always - putting a registry round trip on the critical path
// of every job for an image that is almost certainly already on the node. It
// also means the container silently changes whenever upstream republishes the
// tag, which is not a property a build system's own machinery should have.
//
// Override with FINALIZER_IMAGE (chart: hadesOperator.finalizerImage) when the
// cluster cannot reach Docker Hub, or to point at a mirror. The image needs
// nothing but a shell: it runs `sh -c "echo build finished"`.
const DefaultFinalizerImage = "busybox:1.37.0"

// helper: build a configured PodLogReader for the given namespace/job.
func (r *BuildJobReconciler) podLogReader(namespace, jobID string) k8s.PodLogReader {
	return k8s.PodLogReader{
		K8sClient: r.K8sClient,
		Namespace: namespace,
		JobID:     jobID,
		Publisher: *r.Publisher,
	}
}

// initializeContainerStatuses creates Pending status entries for all expected containers of the BuildJob
func (r *BuildJobReconciler) initializeContainerStatuses(ctx context.Context, bj *buildv1.BuildJob) error {
	slog.Info("Initializing container statuses for BuildJob", "buildJob", bj.Name)
	statuses := make([]buildv1.ContainerStatus, 0, len(bj.Spec.Steps)+1)

	// Initialize status for each step (init containers)
	for _, step := range bj.Spec.Steps {
		statuses = append(statuses, buildv1.ContainerStatus{
			Name:          fmt.Sprintf(BuildStepPrefix, step.ID),
			StepID:        step.ID,
			State:         buildv1.ContainerStatePending,
			LogsPublished: false,
		})
	}

	// Initialize status for finalizer container
	statuses = append(statuses, buildv1.ContainerStatus{
		Name:          FinalizerContainerName,
		StepID:        0, // 0 indicates it's not a step
		State:         buildv1.ContainerStatePending,
		LogsPublished: false,
	})

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh buildv1.BuildJob
		if err := r.Get(ctx, client.ObjectKeyFromObject(bj), &fresh); err != nil {
			return err
		}
		fresh.Status.ContainerStatuses = statuses
		currentStep := int32(1)
		fresh.Status.CurrentStep = &currentStep
		return r.Status().Update(ctx, &fresh)
	})
}

// allTerminatedLogsPublished reports whether every container that has reached a
// terminal state in the pod has had its logs published in the BuildJob status.
// It is the drain gate used before deleting a completed BuildJob: only containers
// that actually terminated (Succeeded/Failed) must be published. Containers that
// never ran (e.g. init steps after a failing step, or the finalizer on a failed
// job) have no terminated state and therefore do not block deletion. A terminated
// container without a matching, published status entry counts as not-yet-drained.
func allTerminatedLogsPublished(pod *corev1.Pod, statuses []buildv1.ContainerStatus) bool {
	published := make(map[string]bool, len(statuses))
	for _, cs := range statuses {
		published[cs.Name] = cs.LogsPublished
	}

	drained := func(containerStatuses []corev1.ContainerStatus) bool {
		for _, cs := range containerStatuses {
			if cs.State.Terminated == nil {
				continue
			}
			if !published[cs.Name] {
				return false
			}
		}
		return true
	}

	return drained(pod.Status.InitContainerStatuses) && drained(pod.Status.ContainerStatuses)
}

// logsDrained reports whether all terminated containers of the BuildJob's pod have
// had their logs published. podGone is true only when the pod is confirmed gone
// (a NotFound result), so the caller proceeds to delete rather than requeue; any
// other error is returned so the caller retries instead of deleting and losing
// logs.
func (r *BuildJobReconciler) logsDrained(ctx context.Context, bj *buildv1.BuildJob) (drained bool, podGone bool, err error) {
	// Re-read the latest status: updateContainerStatuses (run earlier in this
	// reconcile) records the pod name and the freshest LogsPublished flags on a
	// separately fetched object.
	var fresh buildv1.BuildJob
	if err := r.Get(ctx, client.ObjectKeyFromObject(bj), &fresh); err != nil {
		return false, false, err
	}

	podName := fresh.Status.PodName
	if podName == "" {
		// The pod name has not been recorded yet, so we cannot confirm the pod is
		// gone. Do not delete; let the caller requeue (the drain timeout is the
		// backstop against waiting forever).
		return false, false, nil
	}

	p, err := r.K8sClient.CoreV1().Pods(bj.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// The pod is confirmed gone; its logs are unrecoverable.
			return false, true, nil
		}
		// Transient/API/authorization error: propagate so the caller retries
		// instead of deleting the BuildJob and losing logs.
		return false, false, err
	}

	return allTerminatedLogsPublished(p, fresh.Status.ContainerStatuses), false, nil
}

// updateContainerStatuses resolves PodName using the BuildJob and updates each
// BuildJob's container statuses accordingly. It returns draining=true when at
// least one container has terminated but its live log stream has not finished
// publishing, signalling the caller to requeue so the job is not finalized (and
// its pod deleted) before the tail logs are captured.
func (r *BuildJobReconciler) updateContainerStatuses(ctx context.Context, bj *buildv1.BuildJob) (draining bool, pod *corev1.Pod, err error) {
	slog.Info("Updating container statuses for BuildJob", "buildJob", bj.Name)

	pl := r.podLogReader(bj.Namespace, bj.Name)

	podName, err := pl.ResolvePodName(ctx)
	if err != nil {
		slog.Error("Failed to resolve pod name", "error", err)
		return false, nil, err
	}

	p, err := r.K8sClient.CoreV1().Pods(bj.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return false, nil, err
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh buildv1.BuildJob
		if err := r.Get(ctx, client.ObjectKeyFromObject(bj), &fresh); err != nil {
			return err
		}

		// Build map of current statuses from container status slice for easy lookup
		statusMap := make(map[string]buildv1.ContainerStatus)
		for _, cs := range fresh.Status.ContainerStatuses {
			statusMap[cs.Name] = cs
		}

		// Update init container statuses (build steps)
		for _, initCS := range p.Status.InitContainerStatuses {
			statusMap[initCS.Name] = r.updateContainerStateMap(ctx, bj, p, statusMap, initCS)
		}

		// Update regular container statuses (finalizer)
		for _, containerCS := range p.Status.ContainerStatuses {
			statusMap[containerCS.Name] = r.updateContainerStateMap(ctx, bj, p, statusMap, containerCS)
		}

		// Determine current step
		currentStep := r.determineCurrentStep(p, len(bj.Spec.Steps))

		// Convert map back to slice
		newStatuses := make([]buildv1.ContainerStatus, 0, len(statusMap))
		for _, cs := range statusMap {
			newStatuses = append(newStatuses, cs)
		}

		// A terminated container whose logs are not yet published still has a log
		// stream draining; keep requeueing until it finishes.
		draining = false
		for _, cs := range newStatuses {
			if (cs.State == buildv1.ContainerStateSucceeded || cs.State == buildv1.ContainerStateFailed) && !cs.LogsPublished {
				draining = true
				break
			}
		}

		// Update BuildJob status

		fresh.Status.ContainerStatuses = newStatuses
		fresh.Status.CurrentStep = &currentStep
		fresh.Status.PodName = p.Name
		return r.Status().Update(ctx, &fresh)
	})
	return draining, p, err
}

// terminalWaitingReasons are container Waiting.Reason values that do not clear on
// their own, so a pod sitting in one is stuck and its BuildJob should fail.
// Transient reasons (ErrImagePull, ContainerCreating, PodInitializing) are
// intentionally excluded: the backoff/permanent states below only surface after
// the kubelet has already retried, so they act as an implicit grace period and a
// merely slow or first-attempt pull is not mistaken for a failure.
var terminalWaitingReasons = map[string]bool{
	"ImagePullBackOff":           true,
	"ErrImageNeverPull":          true,
	"InvalidImageName":           true,
	"CreateContainerConfigError": true,
	"CreateContainerError":       true,
	"CrashLoopBackOff":           true,
}

// podStuckReason reports whether pod has a container wedged in a terminal waiting
// state (e.g. ImagePullBackOff) and, if so, a concise human-readable reason. Init
// containers (the build steps) are checked first, so a stuck step is reported
// before the app/finalizer container, which sits in PodInitializing until the
// steps finish.
func podStuckReason(pod *corev1.Pod) (string, bool) {
	check := func(statuses []corev1.ContainerStatus, label string) (string, bool) {
		for i, cs := range statuses {
			w := cs.State.Waiting
			if w == nil || !terminalWaitingReasons[w.Reason] {
				continue
			}
			// Keep it concise: the reason plus the offending image and step, not the
			// kubelet's verbose multi-line Waiting.Message (which repeats the pull
			// error in full).
			return fmt.Sprintf("%s (%s %d): %s", w.Reason, label, i+1, cs.Image), true
		}
		return "", false
	}
	if reason, ok := check(pod.Status.InitContainerStatuses, "step"); ok {
		return reason, true
	}
	return check(pod.Status.ContainerStatuses, "container")
}

// updateContainerStateMap updates the ContainerStatus for a specific container
// in the status map and drives its live log stream.
//
// A single follow-stream goroutine is started once the container is running (or,
// after an operator restart or for a very short container, once it is
// terminated) and follows the container to EOF, publishing logs incrementally.
// A terminated container's logs are marked published only after its stream has
// fully drained, so the caller can hold off finalizing the job until then.
func (r *BuildJobReconciler) updateContainerStateMap(ctx context.Context, bj *buildv1.BuildJob, p *corev1.Pod, statusMap map[string]buildv1.ContainerStatus, containerState corev1.ContainerStatus) buildv1.ContainerStatus {
	cs := statusMap[containerState.Name]

	newCS := r.buildContainerStatus(containerState.Name, cs.StepID, containerState.State)
	cs.State = newCS.State

	terminated := cs.State == buildv1.ContainerStateSucceeded || cs.State == buildv1.ContainerStateFailed
	key := logStreamKey(bj.Namespace, bj.Name, cs.Name)

	// If the stream for a still-running container has already finished, it must
	// have failed: a healthy follow stays open until the container stops. Drop the
	// entry so the block below re-establishes it (from the persisted offset)
	// instead of leaving it dead until termination; the running-job requeue drives
	// the retry.
	if !terminated && !cs.LogsPublished {
		if stream := r.logStreams().get(key); stream != nil {
			if finished, streamErr := stream.result(); finished {
				if streamErr != nil {
					slog.Warn("Container log stream failed while running; restarting", "container", cs.Name, "error", streamErr)
				}
				r.logStreams().remove(key)
			}
		}
	}

	// Start (idempotently) the live log stream once the container is running or
	// terminated and its logs have not yet been fully published. The stream
	// captures LogsStreamedUntil at start time so a re-established stream (after
	// an operator restart) skips already-published lines.
	if (cs.State == buildv1.ContainerStateRunning || terminated) && !cs.LogsPublished {
		podName := p.Name
		sinceTime := cs.LogsStreamedUntil
		containerName := cs.Name
		started := r.logStreams().ensureStarted(key, func(streamCtx context.Context, progress func(time.Time)) error {
			pl := r.podLogReader(bj.Namespace, bj.Name)
			return pl.StreamContainerLogs(streamCtx, podName, containerName, sinceTime, progress)
		})
		// Register the container's slot synchronously, in container order (this
		// function is called per container in pod order), so a step producing no
		// output keeps its position even though streams run concurrently. Done
		// once per container, guarded by started.
		if started {
			if err := r.Publisher.PublishJobLog(ctx, buildlogs.Log{JobID: bj.Name, ContainerID: containerName}); err != nil {
				slog.Warn("Failed to register container log slot", "container", containerName, "error", err)
			}
		}
	}

	// Persist streaming progress, throttled, so a restart re-follows from near the
	// last published line rather than from the container's start.
	if stream := r.logStreams().get(key); stream != nil {
		cs.LogsStreamedUntil = throttledStreamOffset(cs.LogsStreamedUntil, stream.progress())
	}

	// Once terminated, mark logs published only after the stream has drained.
	if terminated && !cs.LogsPublished {
		if stream := r.logStreams().get(key); stream != nil {
			if finished, streamErr := stream.result(); finished {
				if streamErr == nil {
					cs.LogsPublished = true
				} else {
					slog.Error("Container log stream failed; will retry", "container", cs.Name, "error", streamErr)
				}
				// Drop the entry either way: on success it is done; on failure the
				// next reconcile re-establishes it (from the persisted offset).
				r.logStreams().remove(key)
			}
		}
	}

	return cs
}

// throttledStreamOffset returns the LogsStreamedUntil value to persist. It keeps
// the previously persisted value unless streaming progress has advanced by at
// least logStreamPersistThrottle, so the field changes rarely and does not cause
// a reconcile/etcd write storm.
func throttledStreamOffset(persisted *metav1.Time, progress time.Time) *metav1.Time {
	if progress.IsZero() {
		return persisted
	}
	if persisted == nil || progress.Sub(persisted.Time) >= logStreamPersistThrottle {
		t := metav1.NewTime(progress)
		return &t
	}
	return persisted
}

// buildContainerStatus maps K8s container state to BuildJob ContainerStatus
func (r *BuildJobReconciler) buildContainerStatus(name string, stepID int32, state corev1.ContainerState) buildv1.ContainerStatus {
	cs := buildv1.ContainerStatus{
		Name:   name,
		StepID: stepID,
	}

	if state.Waiting != nil {
		cs.State = buildv1.ContainerStatePending

	} else if state.Running != nil {
		cs.State = buildv1.ContainerStateRunning

	} else if state.Terminated != nil {
		if state.Terminated.ExitCode == 0 {
			cs.State = buildv1.ContainerStateSucceeded
		} else {
			cs.State = buildv1.ContainerStateFailed
		}
	} else {
		cs.State = buildv1.ContainerStateUnknown
	}

	return cs
}

// determineCurrentStep figures out which step is currently executing
func (r *BuildJobReconciler) determineCurrentStep(pod *corev1.Pod, totalSteps int) int32 {
	// Check init containers (steps)
	for i, initCS := range pod.Status.InitContainerStatuses {
		if initCS.State.Running != nil {
			return int32(i + 1) // Step IDs are 1-based
		}
		if initCS.State.Waiting != nil {
			return int32(i + 1)
		}
	}

	// If all init containers finished, check regular container (finalizer)
	for _, containerCS := range pod.Status.ContainerStatuses {
		if containerCS.State.Running != nil || containerCS.State.Waiting != nil {
			return int32(totalSteps + 1) // Finalizer is the last "step"
		}
	}

	// All done or not started yet
	if len(pod.Status.InitContainerStatuses) == 0 {
		return 1 // Not started
	}
	return int32(totalSteps + 1) // All done
}
