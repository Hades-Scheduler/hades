package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	"github.com/hades-scheduler/hades/hadesScheduler/k8s"
	"github.com/hades-scheduler/hades/shared/buildlogs"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const FinalizerContainerName = "buildjob-finalizer"

// streamEOFGrace is how long a cleanly ended log stream of a container the pod
// still reports as running is left alone before it is treated as broken and
// restarted. Pod status lags the stream's EOF by the kubelet's status sync, which
// is normally well under a second.
const streamEOFGrace = 5 * time.Second

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

// containerSnapshot is what one updateContainerStatuses pass observed and wrote.
// The caller keeps using it for the rest of the reconcile instead of re-reading
// the pod or the BuildJob: the informer cache would hand back the pre-write
// BuildJob, and another pod GET is an API round trip for data already in hand.
type containerSnapshot struct {
	// draining is true while a terminated container's live log stream is still
	// publishing.
	draining bool
	pod      *corev1.Pod
	statuses []buildv1.ContainerStatus
}

// errBuildJobFinalized means the live BuildJob is already terminal or being
// deleted although the cached copy the reconcile started from was not.
var errBuildJobFinalized = errors.New("BuildJob already finalized")

// resolvePod finds the BuildJob's pod. Jobs created by this operator version
// stamp LabelBuildJob on the pod template, so their pods are in the
// label-filtered informer cache and lookups cost no API call. Jobs created
// before that label existed have unlabelled pods the cache never sees; those
// fall back to a live List by the Job controller's job-name label.
func (r *BuildJobReconciler) resolvePod(ctx context.Context, bj *buildv1.BuildJob, job *batchv1.Job) (*corev1.Pod, error) {
	if job.Spec.Template.Labels[LabelBuildJob] == "" {
		return r.podLogReader(bj.Namespace, bj.Name).ResolvePod(ctx)
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(bj.Namespace),
		client.MatchingLabels{LabelBuildJob: bj.Name},
	); err != nil {
		return nil, fmt.Errorf("listing pods of BuildJob %s: %w", bj.Name, err)
	}
	switch len(pods.Items) {
	case 0:
		return nil, fmt.Errorf("pod for BuildJob %s: %w", bj.Name, k8s.ErrPodNotFound)
	case 1:
		return &pods.Items[0], nil
	default:
		return nil, fmt.Errorf("found %d pods for BuildJob %s; expected exactly 1", len(pods.Items), bj.Name)
	}
}

// updateContainerStatuses resolves the BuildJob's pod and updates the BuildJob's
// container statuses accordingly. The returned snapshot has draining=true when
// at least one container has terminated but its live log stream has not
// finished publishing, signalling the caller to requeue so the job is not
// finalized (and its pod deleted) before the tail logs are captured.
func (r *BuildJobReconciler) updateContainerStatuses(ctx context.Context, bj *buildv1.BuildJob, job *batchv1.Job) (containerSnapshot, error) {
	slog.Debug("Updating container statuses for BuildJob", "buildJob", bj.Name)

	p, err := r.resolvePod(ctx, bj, job)
	if err != nil {
		return containerSnapshot{}, err
	}

	snap := containerSnapshot{pod: p}
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Read uncached. The statuses below decide whether a container's log
		// stream is (re)started from LogsStreamedUntil, and pod and stream events
		// trigger reconciles faster than the BuildJob cache catches up with the
		// previous reconcile's write. A stale LogsPublished=false would restart a
		// finished stream and republish its logs.
		var fresh buildv1.BuildJob
		if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(bj), &fresh); err != nil {
			return err
		}
		if !fresh.DeletionTimestamp.IsZero() || phaseFinal(fresh.Status.Phase) {
			return errBuildJobFinalized
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

		currentStep := r.determineCurrentStep(p, len(bj.Spec.Steps))
		newStatuses := orderedContainerStatuses(bj.Spec.Steps, statusMap)

		// A terminated container whose logs are not yet published still has a log
		// stream draining; keep requeueing until it finishes.
		snap.draining = false
		for _, cs := range newStatuses {
			if (cs.State == buildv1.ContainerStateSucceeded || cs.State == buildv1.ContainerStateFailed) && !cs.LogsPublished {
				snap.draining = true
				break
			}
		}
		snap.statuses = newStatuses

		return r.writeContainerStatuses(ctx, &fresh, newStatuses, currentStep, p.Name)
	})
	if err != nil {
		return containerSnapshot{}, err
	}
	return snap, nil
}

// writeContainerStatuses skips the update when nothing changed. Every status write
// bumps the resourceVersion and the watch turns that straight into another
// reconcile, so writing on every pass keeps a running job reconciling in a tight
// loop instead of on the requeue delay.
func (r *BuildJobReconciler) writeContainerStatuses(ctx context.Context, fresh *buildv1.BuildJob, statuses []buildv1.ContainerStatus, currentStep int32, podName string) error {
	if equality.Semantic.DeepEqual(fresh.Status.ContainerStatuses, statuses) &&
		fresh.Status.CurrentStep != nil && *fresh.Status.CurrentStep == currentStep &&
		fresh.Status.PodName == podName {
		return nil
	}

	fresh.Status.ContainerStatuses = statuses
	fresh.Status.CurrentStep = &currentStep
	fresh.Status.PodName = podName
	return r.Status().Update(ctx, fresh)
}

// orderedContainerStatuses returns the statuses in a fixed order: steps in spec
// order, then the finalizer, then anything else sorted by name. Map order is
// random, and an unordered slice would never compare equal to the stored one.
func orderedContainerStatuses(steps []buildv1.BuildStep, statusMap map[string]buildv1.ContainerStatus) []buildv1.ContainerStatus {
	out := make([]buildv1.ContainerStatus, 0, len(statusMap))
	seen := make(map[string]bool, len(statusMap))
	add := func(name string) {
		if cs, ok := statusMap[name]; ok && !seen[name] {
			out = append(out, cs)
			seen[name] = true
		}
	}
	for _, step := range steps {
		add(fmt.Sprintf(BuildStepPrefix, step.ID))
	}
	add(FinalizerContainerName)

	var rest []string
	for name := range statusMap {
		if !seen[name] {
			rest = append(rest, name)
		}
	}
	sort.Strings(rest)
	for _, name := range rest {
		add(name)
	}
	return out
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

	// If the stream for a still-running container has already finished, it either
	// failed or the container exited and the pod status has not caught up yet: the
	// clean end of a follow stream wakes this reconcile immediately, often before
	// the kubelet has reported the termination. Restarting in that window would
	// republish the container's logs, so a clean end is trusted for
	// streamEOFGrace. Past that (or on error) drop the entry so the block below
	// re-establishes it from the persisted offset; the resync requeue drives the
	// retry.
	if !terminated && !cs.LogsPublished {
		if stream := r.logStreams().get(key); stream != nil {
			if finished, at, streamErr := stream.resultAt(); finished && (streamErr != nil || time.Since(at) >= streamEOFGrace) {
				// The kubelet can lag by more than the grace period. Restarting
				// a cleanly ended stream of a container that did exit would
				// republish its logs, so confirm against the live pod first.
				if streamErr == nil && !r.containerLiveRunning(ctx, p, cs.Name) {
					slog.Debug("Log stream ended and live pod shows container stopped; waiting for cache", "container", cs.Name)
				} else {
					slog.Warn("Container log stream ended while running; restarting", "container", cs.Name, "error", streamErr)
					r.logStreams().remove(key)
				}
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
					// Keep the finished entry until stopJob: it is what stops
					// ensureStarted above from following the container again if
					// a later pass works from a BuildJob that does not show
					// LogsPublished yet.
					cs.LogsPublished = true
				} else {
					slog.Error("Container log stream failed; will retry", "container", cs.Name, "error", streamErr)
					// The next reconcile re-establishes it from the persisted offset.
					r.logStreams().remove(key)
				}
			}
		}
	}

	return cs
}

// containerLiveRunning reports whether the live pod (bypassing the cache) still
// shows the named container as running. Any doubt (a failed read, a missing
// pod or container) counts as not running, which only delays a stream restart
// until the next resync instead of risking duplicate logs.
func (r *BuildJobReconciler) containerLiveRunning(ctx context.Context, cached *corev1.Pod, name string) bool {
	var live corev1.Pod
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(cached), &live); err != nil {
		slog.Warn("Failed to read live pod; not restarting log stream yet", "pod", cached.Name, "error", err)
		return false
	}
	for _, list := range [][]corev1.ContainerStatus{live.Status.InitContainerStatuses, live.Status.ContainerStatuses} {
		for _, cs := range list {
			if cs.Name == name {
				return cs.State.Running != nil
			}
		}
	}
	return false
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
