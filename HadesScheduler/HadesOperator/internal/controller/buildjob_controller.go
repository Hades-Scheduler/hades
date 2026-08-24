/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	"github.com/hades-scheduler/hades/hadesScheduler/log"
	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/nats-io/nats.go"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const conflictRequeueDelay = 200 * time.Millisecond
const BuildStepPrefix = "step-%d"

const requeueDelay = 2 * time.Second

// logDrainRequeueDelay is how long to wait before re-reconciling a completed
// BuildJob whose terminated containers still have unpublished logs.
const logDrainRequeueDelay = 1 * time.Second

// logDrainTimeout bounds how long the operator keeps requeuing to drain logs
// before it deletes a completed BuildJob anyway, to avoid an infinite requeue
// when logs can never be published.
const logDrainTimeout = 45 * time.Second

const defaultPriority = 1

const (
	LabelManagedBy = "hades.tum.de/managed-by"
	LabelBuildJob  = "hades.tum.de/buildjob"
	// LabelPriority is the Job/Pod label carrying the job priority. It shares the
	// key with the scheduler's job-metadata contract (hades.MetadataKeyPriority)
	// so the value set by the scheduler surfaces unchanged as a label here.
	LabelPriority = hades.MetadataKeyPriority
)

// ManagedByValue is the value of the LabelManagedBy label applied to Jobs the
// operator owns.
const ManagedByValue = "hades-operator"

// phasePending is the BuildJob status phase for a job that has been created but
// not yet started. Other phases reuse the buildstatus.Status* constants.
const phasePending = "Pending"

// BuildJobReconciler reconciles a BuildJob object
type BuildJobReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	K8sClient        *kubernetes.Clientset
	NatsConnection   *nats.Conn
	DeleteOnComplete bool
	Publisher        *log.NATSPublisher
	MaxParallelism   uint

	// LogStreams tracks live per-container log-follow goroutines. It is created
	// lazily by logStreams() if left nil.
	LogStreams     *logStreamRegistry
	logStreamsOnce sync.Once
}

// logStreams returns the reconciler's log-stream registry, creating it on first
// use so a reconciler constructed without one still works.
func (r *BuildJobReconciler) logStreams() *logStreamRegistry {
	r.logStreamsOnce.Do(func() {
		if r.LogStreams == nil {
			r.LogStreams = NewLogStreamRegistry()
		}
	})
	return r.LogStreams
}

// +kubebuilder:rbac:groups=build.hades.tum.de,resources=buildjobs;buildjobs/status;buildjobs/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods;events;configmaps;secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile ensures the cluster state matches the desired state of a BuildJob.
// It creates/owns a batch Job, updates BuildJob status and each ContainerStatus of the BuildJob, and cleans up on completion.
// Triggers NATS log publishing on ContainerStatus changes, and status publishing on BuildJob status updates.
func (r *BuildJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	// ----------------------------- 0. Retrieve the BuildJob instance -----------------------------
	var bj buildv1.BuildJob
	if err := r.Get(ctx, req.NamespacedName, &bj); err != nil {
		if apierrors.IsNotFound(err) {
			// Object has been deleted; ignore it
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// ----------------------------- 0b. If being deleted, skip (avoid recreating children) --------
	if !bj.DeletionTimestamp.IsZero() {
		r.logStreams().stopJob(bj.Namespace, bj.Name)
		return ctrl.Result{}, nil
	}

	// ----------------------------- 1. Exit if already processed ----------------------------------
	// Only process objects that are not marked as "finalized" (i.e., not deleted)
	if bj.Status.Phase == string(buildstatus.StatusSucceeded) || bj.Status.Phase == string(buildstatus.StatusFailed) {
		r.logStreams().stopJob(bj.Namespace, bj.Name)
		return ctrl.Result{}, nil
	}

	// ----------------------------- 2. Check if the Job already exists ----------------------------
	jobName := fmt.Sprintf(buildlogs.JobNamePrefix, bj.Name)
	var existingJob batchv1.Job
	err := r.Get(ctx, client.ObjectKey{Namespace: bj.Namespace, Name: jobName}, &existingJob)
	if err == nil {
		// Job already exists check the status of the containers. draining is true
		// while a terminated container's live log stream is still publishing.
		draining, pod, err := r.updateContainerStatuses(ctx, &bj)
		if err != nil {
			// When a Job hits its activeDeadlineSeconds (whole-job timeout),
			// Kubernetes marks the Job Failed (reason DeadlineExceeded) and deletes
			// its pod. The pod is then unresolvable and the status update fails - but
			// the Job condition already tells us the outcome, so finalize from it
			// instead of requeueing this error forever. The pod (and thus its logs)
			// is already gone, so there is nothing left to drain.
			if done, succeeded, msg := jobFinished(&existingJob); done {
				slog.Warn("Container status update failed but Job already finished; finalizing", "buildJob", bj.Name, "reason", msg)
				if serr := r.setStatusCompleted(ctx, req.NamespacedName, bj.Name, succeeded, msg); serr != nil {
					if apierrors.IsConflict(serr) {
						return ctrl.Result{RequeueAfter: conflictRequeueDelay}, nil
					}
					return ctrl.Result{}, serr
				}
				r.logStreams().stopJob(bj.Namespace, bj.Name)
				if r.DeleteOnComplete {
					policy := metav1.DeletePropagationForeground
					if derr := r.Delete(ctx, &bj, &client.DeleteOptions{PropagationPolicy: &policy}); derr != nil && !apierrors.IsNotFound(derr) {
						return ctrl.Result{}, derr
					}
				}
				if aerr := r.admitOneSuspendedJob(ctx, bj.Namespace); aerr != nil {
					slog.Error("Failed to admit suspended job after timeout finalization", "error", aerr)
				}
				return ctrl.Result{}, nil
			}

			// draining is unreliable when the status update failed, so requeue
			// instead of proceeding: finalizing here could delete the pod while a
			// container's log stream is still draining and lose its tail logs.
			slog.Error("Failed to update container statuses", "error", err)
			return ctrl.Result{}, fmt.Errorf("updating container statuses: %w", err)
		}

		// A pod wedged in a terminal waiting state (e.g. ImagePullBackOff) never
		// runs, so its Job never gets a Complete/Failed condition and jobFinished
		// would keep the BuildJob "Running" forever. Fail it with the reason and
		// delete the Job (removing the runaway pod and stopping the endless pull
		// retries, which TTLSecondsAfterFinished would never reap on an unfinished
		// Job). No log-drain gate: the containers never started, so there are no
		// logs to wait for.
		if reason, stuck := podStuckReason(pod); stuck {
			slog.Warn("BuildJob pod is stuck; failing", "buildJob", bj.Name, "reason", reason)
			if err := r.setStatusCompleted(ctx, req.NamespacedName, bj.Name, false, reason); err != nil {
				if apierrors.IsConflict(err) {
					return ctrl.Result{RequeueAfter: conflictRequeueDelay}, nil
				}
				return ctrl.Result{}, err
			}
			r.logStreams().stopJob(bj.Namespace, bj.Name)

			policy := metav1.DeletePropagationForeground
			if err := r.Delete(ctx, &existingJob, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			// Remove the CR too only when configured to clean up on completion;
			// otherwise keep it as the failure record (phase Failed, reason in
			// Status.Message).
			if r.DeleteOnComplete {
				if err := r.Delete(ctx, &bj, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			}
			if err := r.admitOneSuspendedJob(ctx, bj.Namespace); err != nil {
				slog.Error("Failed to admit suspended job after stuck-pod failure", "error", err)
			}
			return ctrl.Result{}, nil
		}

		// Job already exists, check the status of the job
		done, succeeded, msg := jobFinished(&existingJob)
		if done {
			// Hold off finalizing (and deleting the pod) until every container's
			// log stream has drained, so no tail logs are lost.
			if draining {
				return ctrl.Result{RequeueAfter: requeueDelay}, nil
			}

			// Drain gate: when the CR (and thus the pod) will be deleted on
			// completion, all logs of terminated containers must be published
			// first, because Kubernetes only serves container logs while the pod
			// exists. This runs BEFORE setStatusCompleted on purpose: setting the
			// terminal phase would trip the early-return guard at the top of
			// Reconcile, so we keep the phase non-terminal and requeue until the
			// logs are drained (or a safety valve fires).
			if r.DeleteOnComplete {
				drained, podGone, drainErr := r.logsDrained(ctx, &bj)
				timedOut := time.Since(jobCompletionTime(&existingJob)) > logDrainTimeout

				if drainErr != nil && !timedOut {
					return ctrl.Result{}, drainErr
				}
				if drainErr == nil && !drained && !podGone && !timedOut {
					slog.Info("Waiting for terminated container logs to be published before deleting", "buildJob", bj.Name)
					return ctrl.Result{RequeueAfter: logDrainRequeueDelay}, nil
				}
				if podGone && !drained {
					slog.Warn("Pod gone before all logs were drained; deleting anyway", "buildJob", bj.Name)
				} else if timedOut && !drained {
					slog.Warn("Timed out waiting for logs to drain; deleting anyway", "buildJob", bj.Name)
				}
			}

			if err := r.setStatusCompleted(ctx, req.NamespacedName, bj.Name, succeeded, msg); err != nil {
				if apierrors.IsConflict(err) {
					return ctrl.Result{RequeueAfter: conflictRequeueDelay}, nil
				}
				return ctrl.Result{}, err
			}

			// Emit the job's overhead/runtime breakdown, derived from the pod's
			// container timestamps. This runs exactly once: setStatusCompleted has
			// set the terminal phase, so the guard at the top of Reconcile
			// short-circuits every later reconcile of this BuildJob.
			r.emitJobTiming(ctx, &bj)

			// All logs are published; stop tracking this job's streams.
			r.logStreams().stopJob(bj.Namespace, bj.Name)

			// If DeleteOnComplete is false, don't delete the CR, used for debugging
			if !r.DeleteOnComplete {
				// If there is available concurrency, admit one suspended job (optional)
				if err := r.admitOneSuspendedJob(ctx, bj.Namespace); err != nil {
					slog.Error("Failed to admit suspended job after completion", "error", err)
				}
				return ctrl.Result{}, nil
			}

			// Delete the CR once the job is done
			policy := metav1.DeletePropagationForeground
			if err := r.Delete(ctx, &bj, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil {
				return ctrl.Result{}, err
			}

			// Once a old job is deleted, admit one suspended job
			if err := r.admitOneSuspendedJob(ctx, bj.Namespace); err != nil {
				slog.Error("Failed to admit suspended job after deletion", "error", err)
			}
			return ctrl.Result{}, nil
		}

		// Build is not done, set the status to be "running"
		if bj.Status.Phase != string(buildstatus.StatusRunning) {
			if err := r.setStatusRunning(ctx, req.NamespacedName, jobName, bj.Name); err != nil {
				if apierrors.IsConflict(err) {
					return ctrl.Result{RequeueAfter: conflictRequeueDelay}, nil
				}
				return ctrl.Result{}, err
			}
		}

		// Requeue while the job runs. The operator watches only BuildJobs and owned
		// Jobs, not Pods, so a Job whose init containers are progressing produces no
		// events between creation and completion. Periodic requeues are how the
		// operator observes each container entering Running (to start its live log
		// stream) and terminating (to finalize once its stream drains).
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	// ----------------------------- 3. First-time creation -----------------------------
	// 3.0 check concurrency limit and current active jobs
	active, err := r.countActiveJobs(ctx, bj.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	shouldSuspend := active >= r.MaxParallelism

	// 3.1 Create Kubernetes Job (initContainers = bj.Spec.Steps)
	k8sJob := buildK8sJob(&bj, jobName, r.DeleteOnComplete, shouldSuspend)

	// 3.2 Set OwnerReference
	if err := controllerutil.SetControllerReference(&bj, k8sJob, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	// 3.3 Create Job in Kubernetes as Pod
	slog.Info("Creating Job for BuildJob", "job", k8sJob.Name)

	if err := r.Create(ctx, k8sJob); err != nil {
		if apierrors.IsAlreadyExists(err) {
			slog.Debug("Job already exists, ", "job", k8sJob.Name)
		} else {
			slog.Error("cannot create Job", "error", err)
			return ctrl.Result{}, err
		}
	}

	// 3.3.1 Initialize container statuses
	if err := r.initializeContainerStatuses(ctx, &bj); err != nil {
		slog.Error("Failed to initialize container statuses", "error", err)
	}

	// 3.4 Update CR Status → Running
	// 3.4.1 If over the concurrency limit, set to Pending and requeue
	if shouldSuspend {
		if err := r.setStatusPending(ctx, req.NamespacedName,
			fmt.Sprintf("Waiting for capacity: active=%d, limit=%d", active, r.MaxParallelism),
		); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: conflictRequeueDelay}, nil
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	//3.4.2 Otherwise, set to Running
	if err := r.setStatusRunning(ctx, req.NamespacedName, jobName, bj.Name); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: conflictRequeueDelay}, nil
		}
		return ctrl.Result{}, err
	}

	// Requeue so the operator observes container transitions and drives live log
	// streaming while the Job runs (Pods are not watched directly).
	return ctrl.Result{RequeueAfter: requeueDelay}, nil
}

// setStatusRunning sets BuildJob.Status to "Running", records StartTime and PodName.
// publishes "running" jobstatus to NATS.
// Uses optimistic concurrency (RetryOnConflict).
func (r *BuildJobReconciler) setStatusRunning(ctx context.Context, nn types.NamespacedName, jobName string, jobID string) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &buildv1.BuildJob{}
		if err := r.Get(ctx, nn, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.DeletionTimestamp != nil {
			return nil
		}
		if latest.Status.Phase == string(buildstatus.StatusRunning) {
			return nil
		}

		base := latest.DeepCopy()
		now := metav1.Now()
		latest.Status.Phase = string(buildstatus.StatusRunning)
		latest.Status.StartTime = &now
		latest.Status.PodName = jobName

		if err := r.Status().Patch(ctx, latest, client.MergeFrom(base)); err != nil {
			slog.Error("Failed to patch BuildJob status to Running", "error", err)
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	if err := r.Publisher.PublishJobStatus(ctx, buildstatus.StatusRunning, jobID); err != nil {
		slog.Error("Failed to publish job running status", "job_id", jobID, "error", err)
		return err
	}
	slog.Info("Published job running status", "job_id", jobID)
	return nil
}

// setStatusCompleted marks BuildJob.Status as Succeeded/Failed and sets CompletionTime/message.
// publishes "success" or "failed" jobstatus to NATS.
// Uses optimistic concurrency (RetryOnConflict).
func (r *BuildJobReconciler) setStatusCompleted(ctx context.Context, nn types.NamespacedName, jobID string, succeeded bool, msg string) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &buildv1.BuildJob{}
		if err := r.Get(ctx, nn, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.DeletionTimestamp != nil {
			return nil
		}

		if latest.Status.Phase == string(buildstatus.StatusSucceeded) || latest.Status.Phase == string(buildstatus.StatusFailed) {
			return nil
		}

		base := latest.DeepCopy()
		now := metav1.Now()
		if succeeded {
			latest.Status.Phase = string(buildstatus.StatusSucceeded)
		} else {
			latest.Status.Phase = string(buildstatus.StatusFailed)
		}
		latest.Status.Message = msg
		latest.Status.CompletionTime = &now

		return r.Status().Patch(ctx, latest, client.MergeFrom(base))
	}); err != nil {
		return err
	}

	if succeeded {
		if err := r.Publisher.PublishJobStatus(ctx, buildstatus.StatusSucceeded, jobID); err != nil {
			slog.Error("Failed to publish job succeeded status", "job_id", jobID, "error", err)
			return err
		}
		slog.Info("Published job succeeded status", "job_id", jobID)
		return nil
	} else {
		if err := r.Publisher.PublishJobStatus(ctx, buildstatus.StatusFailed, jobID, msg); err != nil {
			slog.Error("Failed to publish job failed status", "job_id", jobID, "error", err)
			return err
		}
		slog.Info("Published job failed status", "job_id", jobID)
		return nil
	}
}

// setStatusPending marks BuildJob.Status as Pending with a message.
// Uses optimistic concurrency (RetryOnConflict).
func (r *BuildJobReconciler) setStatusPending(ctx context.Context, nn types.NamespacedName, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &buildv1.BuildJob{}
		if err := r.Get(ctx, nn, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.DeletionTimestamp != nil {
			return nil
		}
		if latest.Status.Phase == phasePending {
			return nil
		}
		base := latest.DeepCopy()
		now := metav1.Now()
		latest.Status.Phase = phasePending
		latest.Status.Message = msg
		if latest.Status.StartTime == nil {
			latest.Status.StartTime = &now
		}
		return r.Status().Patch(ctx, latest, client.MergeFrom(base))
	})
}

// buildK8sJob creates a one-time Job with InitContainers that execute each Step in order
func buildK8sJob(bj *buildv1.BuildJob, jobName string, deleteOnComplete bool, suspend bool) *batchv1.Job {
	sharedVolName := "shared"
	sharedMount := corev1.VolumeMount{Name: sharedVolName, MountPath: "/shared"}

	var initCtrs []corev1.Container
	for _, s := range bj.Spec.Steps {
		// Merge job-level metadata (available to all steps) with step metadata,
		// letting step metadata override job metadata on key collision. Reserved
		// keys are set last so they always win. Matches Docker semantics.
		env := map[string]string{}
		maps.Copy(env, bj.Spec.Metadata)
		maps.Copy(env, s.Metadata)
		env["UUID"] = bj.Name
		env["JOB_NAME"] = bj.Spec.Name

		// A continueOnError step passes its script via HADES_STEP_SCRIPT (see
		// below). Set it in the map before conversion so it is a reserved key
		// (overriding any metadata of the same name) and never produces a
		// duplicate env var.
		hasScript := strings.TrimSpace(s.Script) != ""
		if hasScript && s.ContinueOnError {
			env["HADES_STEP_SCRIPT"] = s.Script
		}

		c := corev1.Container{
			Name:         fmt.Sprintf(BuildStepPrefix, s.ID),
			Image:        s.Image,
			Env:          envFromMeta(env),
			VolumeMounts: []corev1.VolumeMount{sharedMount},
		}

		if hasScript {
			c.Command = []string{"/bin/sh", "-c"}
			if s.ContinueOnError {
				// Init containers have no native "continue on error": a non-zero
				// exit aborts the pod and skips the remaining steps. Run the
				// script in a nested shell whose exit status is discarded (|| true)
				// so this container always exits 0 and later steps (e.g. the
				// result parser) still run. This is robust even when the script
				// uses `set -e` or ends in an explicit non-zero `exit`, both of
				// which would bypass a naive "<script>; exit 0". The script is
				// passed via the HADES_STEP_SCRIPT env var (set above) to avoid
				// shell-quoting issues.
				c.Args = []string{`/bin/sh -c "$HADES_STEP_SCRIPT" || true`}
			} else {
				c.Args = []string{s.Script}
			}
		}

		// Set resource limits if specified
		if s.CPULimit != nil || s.MemoryLimit != nil {
			c.Resources.Limits = corev1.ResourceList{}
			if s.CPULimit != nil {
				c.Resources.Limits[corev1.ResourceCPU] = *s.CPULimit
			}
			if s.MemoryLimit != nil {
				c.Resources.Limits[corev1.ResourceMemory] = *s.MemoryLimit
			}
		}

		// Set resource requests if specified
		if s.CPURequest != nil || s.MemoryRequest != nil {
			c.Resources.Requests = corev1.ResourceList{}
			if s.CPURequest != nil {
				c.Resources.Requests[corev1.ResourceCPU] = *s.CPURequest
			}
			if s.MemoryRequest != nil {
				c.Resources.Requests[corev1.ResourceMemory] = *s.MemoryRequest
			}
		}

		// If requests are not set, but limits are, set requests = limits
		if len(c.Resources.Requests) == 0 && len(c.Resources.Limits) > 0 {
			c.Resources.Requests = corev1.ResourceList{}
			for k, v := range c.Resources.Limits {
				c.Resources.Requests[k] = v
			}
		}

		// NOTE: s.Network, s.MemorySwap and s.PidsLimit are intentionally NOT applied
		// here. They are Docker-executor-only: Kubernetes has no per-container swap or
		// PID-limit field, and all containers in a pod share one network namespace, so
		// per-step network isolation is not expressible. These fields are accepted for
		// schema parity with the Docker executor only. The whole-job timeout is enforced
		// via Job.spec.activeDeadlineSeconds (see below).

		initCtrs = append(initCtrs, c)
	}

	// Add a dummy container that runs after all init containers have finished
	dummy := corev1.Container{
		Name:         FinalizerContainerName,
		Image:        "busybox",
		Command:      []string{"sh", "-c", "echo build finished"},
		VolumeMounts: []corev1.VolumeMount{sharedMount},
	}

	podSpec := corev1.PodSpec{
		InitContainers: initCtrs,
		Containers:     []corev1.Container{dummy},
		Volumes: []corev1.Volume{{
			Name: sharedVolName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		}},
		RestartPolicy: corev1.RestartPolicyNever,
	}

	ttl := (*int32)(nil)
	backoff := int32(0)

	if !deleteOnComplete {
		t := int32(3600)
		ttl = &t
	}

	labels := map[string]string{
		LabelManagedBy: ManagedByValue,
		LabelBuildJob:  bj.Name,
	}

	if p, ok := bj.Labels[LabelPriority]; ok && strings.TrimSpace(p) != "" {
		labels[LabelPriority] = p
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: bj.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			Suspend:                 &suspend,
			TTLSecondsAfterFinished: ttl,
			BackoffLimit:            &backoff,
			// Whole-job timeout: Kubernetes fails the Job (reason DeadlineExceeded)
			// and deletes its pod once this elapses, covering time spent in init
			// containers (the build steps). nil when no timeout is configured.
			ActiveDeadlineSeconds: bj.Spec.TimeoutSeconds,
			Template: corev1.PodTemplateSpec{
				Spec: podSpec,
			},
		},
	}
}

// validEnvName matches the names Kubernetes accepts for a container environment
// variable: C_IDENTIFIER-ish, per the API server's own validation regex.
var validEnvName = regexp.MustCompile(`^[-._a-zA-Z][-._a-zA-Z0-9]*$`)

// envFromMeta converts a string map to []corev1.EnvVar for container env injection.
// Keys are sorted so the generated env order is deterministic.
//
// Keys that Kubernetes would reject as environment variable names are skipped.
// Job metadata is a general-purpose string map and Hades itself puts
// domain-qualified keys in it - "hades.tum.de/priority" and
// "hades.tum.de/priorityName" are injected by the NATS consumer for every job.
// A "/" is legal in a label and legal in a Docker env name, but not in a
// Kubernetes one, so passing them through made the API server reject the whole
// Job: every job submitted with a priority failed to run on the Kubernetes
// executor, while the same job ran fine on the Docker executor.
//
// Skipping rather than sanitising is deliberate: rewriting a key would invent
// an env var the submitter never asked for and could silently collide with a
// real one. The values are still available where they belong - priority is
// carried as a Job/Pod label by the scheduler.
func envFromMeta(m map[string]string) []corev1.EnvVar {
	keys := make([]string, 0, len(m))
	for k := range m {
		if !validEnvName.MatchString(k) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	envs := make([]corev1.EnvVar, 0, len(keys))
	for _, k := range keys {
		envs = append(envs, corev1.EnvVar{Name: k, Value: m[k]})
	}
	return envs
}

// SetupWithManager registers the controller, watching BuildJobs and owned Jobs.
func (r *BuildJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&buildv1.BuildJob{}).
		Owns(&batchv1.Job{}).
		Named("buildjob").
		Complete(r)
}

// jobFinished checks Job conditions and reports terminal state and reason.
func jobFinished(k8sJob *batchv1.Job) (done bool, succeeded bool, reason string) {
	for _, c := range k8sJob.Status.Conditions {
		switch c.Type {
		case batchv1.JobComplete:
			if c.Status == corev1.ConditionTrue {
				return true, true, c.Message
			}
		case batchv1.JobFailed:
			if c.Status == corev1.ConditionTrue {
				return true, false, c.Message
			}
		}
	}
	return false, false, ""
}

// jobCompletionTime returns a best-effort timestamp for when the Job finished.
// It prefers Status.CompletionTime (set for completed Jobs), falls back to the
// latest true Complete/Failed condition transition time (failed Jobs have no
// CompletionTime), and finally to now() when nothing is available so callers do
// not treat an unknown completion time as already timed out.
func jobCompletionTime(k8sJob *batchv1.Job) time.Time {
	if k8sJob.Status.CompletionTime != nil {
		return k8sJob.Status.CompletionTime.Time
	}
	var latest time.Time
	for _, c := range k8sJob.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		if c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed {
			if c.LastTransitionTime.Time.After(latest) {
				latest = c.LastTransitionTime.Time
			}
		}
	}
	if latest.IsZero() {
		return time.Now()
	}
	return latest
}

// countActiveJobs counts the number of non-completed, non-suspended Jobs in the given namespace.
func (r *BuildJobReconciler) countActiveJobs(ctx context.Context, namespace string) (uint, error) {
	var jl batchv1.JobList
	if err := r.List(ctx, &jl,
		client.InNamespace(namespace),
		client.MatchingLabels{LabelManagedBy: ManagedByValue},
	); err != nil {
		return 0, err
	}
	active := 0
	for _, j := range jl.Items {
		done, _, _ := jobFinished(&j)
		if done {
			continue
		}
		// Unless the job is completed or suspended, count it as active
		if j.Spec.Suspend == nil || !*j.Spec.Suspend {
			active++
		}
	}
	return uint(active), nil
}

// admitOneSuspendedJob unsuspends the oldest suspended Job in the given namespace, if any.
func (r *BuildJobReconciler) admitOneSuspendedJob(ctx context.Context, namespace string) error {
	var jl batchv1.JobList
	if err := r.List(ctx, &jl,
		client.InNamespace(namespace),
		client.MatchingLabels{LabelManagedBy: ManagedByValue},
	); err != nil {
		return err
	}

	pick, bestPri := r.pickBestSuspendedJob(ctx, jl.Items)
	if pick == nil {
		return nil
	}

	base := pick.DeepCopy()
	f := false
	pick.Spec.Suspend = &f

	slog.Info("Admitting suspended Job", "job", pick.Name, "priority", bestPri)

	return r.Patch(ctx, pick, client.MergeFrom(base))
}

func (r *BuildJobReconciler) pickBestSuspendedJob(ctx context.Context, jobs []batchv1.Job) (*batchv1.Job, int) {
	var pick *batchv1.Job
	bestPri := -1

	for i := range jobs {
		j := &jobs[i]

		done, _, _ := jobFinished(j)
		if done || j.Spec.Suspend == nil || !*j.Spec.Suspend {
			continue
		}

		pri := r.priorityForJob(ctx, j)
		if pick == nil || pri > bestPri || (pri == bestPri && j.CreationTimestamp.Before(&pick.CreationTimestamp)) {
			pick = j
			bestPri = pri
		}
	}

	return pick, bestPri
}

func (r *BuildJobReconciler) priorityForJob(ctx context.Context, j *batchv1.Job) int {
	pri := priorityFromLabels(j.Labels, defaultPriority) // default priority is 1 (lowest priority)
	if pri != defaultPriority {
		return pri
	}
	if bjName, ok := j.Labels[LabelBuildJob]; ok && bjName != "" {
		var bj buildv1.BuildJob
		if err := r.Get(ctx, types.NamespacedName{Namespace: j.Namespace, Name: bjName}, &bj); err == nil {
			return priorityFromLabels(bj.Labels, defaultPriority)
		}
	}
	return 1
}

func priorityFromLabels(labels map[string]string, defaultVal int) int {
	if labels == nil {
		return defaultVal
	}
	if s, ok := labels[LabelPriority]; ok {
		if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && v >= 1 {
			return v
		}
	}
	return defaultVal
}
