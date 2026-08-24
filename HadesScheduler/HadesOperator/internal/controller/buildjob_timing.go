package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	"github.com/hades-scheduler/hades/hadesScheduler/k8s"
	"github.com/hades-scheduler/hades/shared/timing"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// maxReconcileLag caps reconcile_detection_lag. In steady state the gap between
// the last step finishing and the operator observing completion is the requeue
// poll (2s) plus the finalizer and log-drain gate (up to ~45s). After an
// operator restart or a reconcile backlog it can be far larger; capping keeps a
// single anomalous job from dominating the overhead rollup.
const maxReconcileLag = 2 * time.Minute

// emitJobTiming records a completed BuildJob's overhead/runtime breakdown,
// derived from the pod's init-container timestamps (which Kubernetes records but
// Hades otherwise never reads). It is best-effort: any failure is logged and the
// job is unaffected. Called once, at the terminal transition.
//
// Accuracy note: Kubernetes stores container timestamps to whole-second
// precision, so sub-second steps read as 0-1s here; the Docker executor's
// phases stay millisecond-precise.
func (r *BuildJobReconciler) emitJobTiming(ctx context.Context, bj *buildv1.BuildJob) {
	// Continue the trace propagated from the API, if any. Skip Extract for an
	// absent annotation so an empty carrier does not touch the context.
	traceCtx := ctx
	if tp := bj.Annotations[k8s.AnnotationTraceParent]; tp != "" {
		traceCtx = timing.Extract(ctx, map[string]string{"traceparent": tp})
	}

	// The job's phases are reconstructed from timestamps after completion, so the
	// root span must begin at the job's start (submission, else CR creation);
	// a now() root would start after its own backdated child spans.
	start := bj.CreationTimestamp.Time
	var submittedAt time.Time
	if v := bj.Annotations[k8s.AnnotationSubmittedAt]; v != "" {
		if s, err := time.Parse(time.RFC3339Nano, v); err == nil {
			submittedAt = s
			if s.Before(start) {
				start = s
			}
		}
	}

	timer := timing.NewJobTimer(traceCtx, "k8s", bj.Name, start)
	defer timer.Summary()

	// queue_wait: API submission -> CR creation. Crosses hosts, so a skewed clock
	// is clamped to zero by Record.
	if !submittedAt.IsZero() {
		timer.Record(0, timing.PhaseQueueWait, submittedAt, bj.CreationTimestamp.Time)
	}

	podName := bj.Status.PodName
	if podName == "" {
		return
	}
	pod, err := r.K8sClient.CoreV1().Pods(bj.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		slog.Warn("timing: failed to fetch pod for job timing", "error", err, "buildJob", bj.Name)
		return
	}

	recordStepTiming(timer, bj, pod.Status.InitContainerStatuses)
}

// recordStepTiming derives the per-step and job-level phases from the pod's
// init-container statuses. It matches statuses to steps by container name rather
// than slice position, because the Kubernetes API does not guarantee
// initContainerStatuses is ordered to match the spec; iterating the spec in
// order keeps prevFinished and the step IDs correct. Separated from the pod
// fetch so it can be unit tested with synthetic statuses.
func recordStepTiming(timer *timing.JobTimer, bj *buildv1.BuildJob, initStatuses []corev1.ContainerStatus) {
	byName := make(map[string]corev1.ContainerStatus, len(initStatuses))
	for _, cs := range initStatuses {
		byName[cs.Name] = cs
	}

	var prevFinished time.Time
	provisioned := false
	for _, step := range bj.Spec.Steps {
		cs, ok := byName[fmt.Sprintf(BuildStepPrefix, step.ID)]
		if !ok {
			continue
		}
		term := cs.State.Terminated
		if term == nil {
			// Step never ran (an earlier step failed and the pod aborted), so it
			// has no runtime to attribute.
			continue
		}
		started := term.StartedAt.Time
		finished := term.FinishedAt.Time
		stepID := int(step.ID)

		if !provisioned {
			// provision: CR creation -> first executed step starting (scheduling +
			// first image pull). Later steps' waits are step_wait.
			timer.Record(0, timing.PhaseProvision, bj.CreationTimestamp.Time, started)
			provisioned = true
		} else {
			timer.Record(stepID, timing.PhaseStepWait, prevFinished, started)
		}
		timer.Record(stepID, timing.PhaseStepRun, started, finished)
		prevFinished = finished
	}

	// reconcile_detection_lag: last step finishing -> the operator observing
	// completion now. Dominated by the requeue poll and log-drain gate, so it is a
	// distinct phase rather than silently inflating teardown. Capped so an
	// operator restart or backlog cannot make one job dominate the overhead
	// rollup.
	if !prevFinished.IsZero() {
		lag := time.Since(prevFinished)
		if lag > maxReconcileLag {
			slog.Warn("timing: reconcile detection lag capped", "buildJob", bj.Name, "raw", lag, "cap", maxReconcileLag)
			lag = maxReconcileLag
		}
		timer.Record(0, timing.PhaseReconcileLag, prevFinished, prevFinished.Add(lag))
	}
}
