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
	"errors"
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
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	"github.com/hades-scheduler/hades/hadesScheduler/k8s"
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

// DefaultRequeueDelay is the safety resync interval used when
// BuildJobReconciler.RequeueDelay is not set to a positive value. Progress is
// driven by watch events (the BuildJob, its Job, its pod, and finished log
// streams), so this only bounds how late the operator notices something those
// events missed: a pod created before pods carried LabelBuildJob, a log stream
// that failed and needs restarting, or a suspended Job whose admission was
// missed.
const DefaultRequeueDelay = 15 * time.Second

// podPendingRequeueDelay is how soon a BuildJob whose unsuspended Job has no pod
// yet is looked at again. The pod's creation event normally gets there first;
// this covers a pod the informer cache has not seen yet.
const podPendingRequeueDelay = 1 * time.Second

// DefaultMaxConcurrentReconciles is used when
// BuildJobReconciler.MaxConcurrentReconciles is not positive. Reconciles spend
// most of their time waiting on the API server and NATS, so a single worker
// would queue every running job's status updates behind each other.
const DefaultMaxConcurrentReconciles = 8

// admissionCacheGrace bounds how long an admission the informer cache does not
// show yet is still counted as active. It only has to outlast the cache's watch
// lag; if the Job vanished instead (deleted by hand), the slot frees after this.
const admissionCacheGrace = 30 * time.Second

// selfAdmitInterval coalesces the admission passes triggered by suspended
// BuildJobs themselves. Each pass lists every Job in the namespace under the
// admission lock, so letting each of S suspended BuildJobs run its own pass on
// every resync would cost S passes and starve the running jobs' reconciles.
// Completions bypass this: they free a slot and must always admit.
const selfAdmitInterval = 1 * time.Second

// logDrainRequeueDelay is how long to wait before re-reconciling a completed
// BuildJob whose terminated containers still have unpublished logs.
const logDrainRequeueDelay = 1 * time.Second

// DefaultLogDrainTimeout is used when BuildJobReconciler.LogDrainTimeout is not
// set to a positive value. It bounds how long the operator keeps requeuing to
// drain logs before it deletes a completed BuildJob anyway, to avoid an infinite
// requeue when logs can never be published.
const DefaultLogDrainTimeout = 45 * time.Second

const defaultPriority = 1

// podTerminationGracePeriodSeconds is the build pod's grace period between
// SIGTERM and SIGKILL. A build has nothing to flush on shutdown.
const podTerminationGracePeriodSeconds = 5

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
	// APIReader reads straight from the API server (mgr.GetAPIReader()). It is
	// used where acting on a stale cached BuildJob would publish something twice
	// (container log streams and the job status transitions) and for admission
	// counting, where a stale cache let the limit stop binding under a burst
	// (measured at 59 concurrent Jobs against MaxParallelism=16). Nil falls back
	// to Client, which is what the unit tests' fake client provides anyway.
	APIReader        client.Reader
	Scheme           *runtime.Scheme
	K8sClient        kubernetes.Interface
	NatsConnection   *nats.Conn
	DeleteOnComplete bool
	Publisher        *log.NATSPublisher
	MaxParallelism   uint

	// RequeueDelay is the safety resync for a BuildJob whose Job is still
	// progressing. Zero or negative falls back to DefaultRequeueDelay.
	RequeueDelay time.Duration
	// MaxConcurrentReconciles is the number of BuildJobs reconciled in parallel.
	// Zero or negative falls back to DefaultMaxConcurrentReconciles.
	MaxConcurrentReconciles int
	// LogDrainTimeout bounds how long a completed BuildJob is held while its
	// container logs drain. Zero or negative falls back to DefaultLogDrainTimeout.
	LogDrainTimeout time.Duration

	// FinalizerImage is the image for the pod's main container. Empty falls back
	// to DefaultFinalizerImage.
	FinalizerImage string

	// ImagePullPolicy is applied to every step container. Empty leaves it unset,
	// which keeps Kubernetes' default (Always for :latest or untagged images).
	ImagePullPolicy corev1.PullPolicy

	// LogStreams tracks live per-container log-follow goroutines. It is created
	// lazily by logStreams() if left nil.
	LogStreams     *logStreamRegistry
	logStreamsOnce sync.Once

	admission admissionTracker
}

// admissionTracker serializes MaxParallelism decisions across concurrent
// reconciles. Counting active Jobs and then creating or unsuspending one is a
// check-then-act on the informer cache: two workers could both see a free slot,
// and a reader that lags behind the API server (the cache, which is what the
// APIReader fallback gives) shows a worker its own earlier admission only once
// the watch event lands. mu closes the first gap and pending the second.
type admissionTracker struct {
	mu sync.Mutex
	// pending holds Jobs (namespace/name) admitted by this process that the
	// cache may still show as suspended or not at all, with the admission time.
	pending map[string]time.Time
	// lastPass is when admitSuspendedJobs last ran per namespace, so the
	// self-admission attempts of many suspended BuildJobs coalesce into one pass.
	lastPass map[string]time.Time
}

func admissionKey(namespace, name string) string {
	return namespace + "/" + name
}

// markAdmitted records an admission. Callers must hold mu.
func (a *admissionTracker) markAdmitted(namespace, name string, now time.Time) {
	if a.pending == nil {
		a.pending = make(map[string]time.Time)
	}
	a.pending[admissionKey(namespace, name)] = now
}

// isPending reports whether a Job was admitted but the cache does not show it
// yet. Callers must hold mu.
func (a *admissionTracker) isPending(namespace, name string) bool {
	_, ok := a.pending[admissionKey(namespace, name)]
	return ok
}

// activeCount counts the namespace's running (non-finished, non-suspended) Jobs
// in jobs, plus admissions the cache does not reflect yet. It forgets pending
// entries once the cache has caught up or admissionCacheGrace has passed.
// Callers must hold mu.
func (a *admissionTracker) activeCount(namespace string, jobs []batchv1.Job, now time.Time) uint {
	var active uint
	for i := range jobs {
		j := &jobs[i]
		done, _, _ := jobFinished(j)
		suspended := jobSuspended(j)
		if done || !suspended {
			delete(a.pending, admissionKey(j.Namespace, j.Name))
		}
		if !done && !suspended {
			active++
		}
	}
	prefix := namespace + "/"
	for key, at := range a.pending {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if now.Sub(at) > admissionCacheGrace {
			delete(a.pending, key)
			continue
		}
		active++
	}
	return active
}

// apiReader returns the uncached reader, falling back to the client.
func (r *BuildJobReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
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

// requeueDelay returns the configured poll interval, falling back to the default
// when unset or non-positive. A non-positive RequeueAfter would make
// controller-runtime requeue immediately and busy-loop the controller.
func (r *BuildJobReconciler) requeueDelay() time.Duration {
	return positiveOrDefault(r.RequeueDelay, DefaultRequeueDelay)
}

// maxConcurrentReconciles returns the configured worker count, falling back to
// the default when unset or non-positive.
func (r *BuildJobReconciler) maxConcurrentReconciles() int {
	if r.MaxConcurrentReconciles <= 0 {
		return DefaultMaxConcurrentReconciles
	}
	return r.MaxConcurrentReconciles
}

// logDrainTimeout returns the configured log-drain safety valve, falling back to
// the default when unset or non-positive.
func (r *BuildJobReconciler) logDrainTimeout() time.Duration {
	return positiveOrDefault(r.LogDrainTimeout, DefaultLogDrainTimeout)
}

// finalizerImage returns the configured image for the pod's main container,
// falling back to the default when unset. Whitespace-only is treated as unset:
// an empty image would be rejected by the API server for every job, which is a
// worse failure than ignoring a blank setting.
func (r *BuildJobReconciler) finalizerImage() string {
	if strings.TrimSpace(r.FinalizerImage) == "" {
		return DefaultFinalizerImage
	}
	return r.FinalizerImage
}

// ParseImagePullPolicy maps K8S_IMAGE_PULL_POLICY to the Kubernetes constant. It
// uses the same always / if-not-present / never values as the Docker executor's
// DOCKER_IMAGE_PULL_POLICY. Empty means "leave unset". Unknown values fail instead
// of falling back, so a typo doesn't quietly do nothing.
func ParseImagePullPolicy(s string) (corev1.PullPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return "", nil
	case "always":
		return corev1.PullAlways, nil
	case "if-not-present":
		return corev1.PullIfNotPresent, nil
	case "never":
		return corev1.PullNever, nil
	default:
		return "", fmt.Errorf("invalid image pull policy %q: expected one of \"always\", \"if-not-present\", \"never\" (or empty)", s)
	}
}

// positiveOrDefault returns d when it is positive, otherwise fallback.
func positiveOrDefault(d, fallback time.Duration) time.Duration {
	if d <= 0 {
		return fallback
	}
	return d
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
	if phaseFinal(bj.Status.Phase) {
		r.logStreams().stopJob(bj.Namespace, bj.Name)
		return ctrl.Result{}, nil
	}

	// ----------------------------- 2. Check if the Job already exists ----------------------------
	jobName := fmt.Sprintf(buildlogs.JobNamePrefix, bj.Name)
	var existingJob batchv1.Job
	err := r.Get(ctx, client.ObjectKey{Namespace: bj.Namespace, Name: jobName}, &existingJob)
	if err == nil {
		// A suspended Job has no pod yet. It is normally admitted when another
		// job finishes, but with several workers that admission can race this
		// Job's creation and miss it, so a suspended Job also tries to admit
		// itself (in priority order, via admitSuspendedJobs) whenever it is
		// reconciled.
		if jobSuspended(&existingJob) {
			if done, _, _ := jobFinished(&existingJob); !done {
				if err := r.selfAdmitSuspendedJobs(ctx, bj.Namespace); err != nil {
					slog.Error("Failed to admit suspended jobs", "buildJob", bj.Name, "error", err)
				}
				return ctrl.Result{RequeueAfter: r.requeueDelay()}, nil
			}
		}

		// Job already exists check the status of the containers. snap.draining is
		// true while a terminated container's live log stream is still publishing.
		snap, err := r.updateContainerStatuses(ctx, &bj, &existingJob)
		if errors.Is(err, errBuildJobFinalized) {
			// The cached BuildJob was stale: another reconcile already finalized it.
			r.logStreams().stopJob(bj.Namespace, bj.Name)
			return ctrl.Result{}, nil
		}
		if err != nil {
			// When a Job hits its activeDeadlineSeconds (whole-job timeout),
			// Kubernetes marks the Job Failed (reason DeadlineExceeded) and deletes
			// its pod. The pod is then unresolvable and the status update fails - but
			// the Job condition already tells us the outcome, so finalize from it
			// instead of requeueing this error forever. The pod (and thus its logs)
			// is already gone, so there is nothing left to drain.
			if done, succeeded, msg := jobFinished(&existingJob); done {
				slog.Warn("Container status update failed but Job already finished; finalizing", "buildJob", bj.Name, "reason", msg)
				if _, serr := r.setStatusCompleted(ctx, req.NamespacedName, bj.Name, succeeded, msg); serr != nil {
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
				if aerr := r.admitSuspendedJobs(ctx, bj.Namespace); aerr != nil {
					slog.Error("Failed to admit suspended job after timeout finalization", "error", aerr)
				}
				return ctrl.Result{}, nil
			}

			// A just-created or just-admitted Job has no pod for a moment. That
			// is not a failure, and returning an error here would put the
			// BuildJob on exponential backoff right when it is about to start.
			if errors.Is(err, k8s.ErrPodNotFound) {
				slog.Debug("Pod not created yet", "buildJob", bj.Name)
				return ctrl.Result{RequeueAfter: podPendingRequeueDelay}, nil
			}

			// draining is unreliable when the status update failed, so requeue
			// instead of proceeding: finalizing here could delete the pod while a
			// container's log stream is still draining and lose its tail logs.
			slog.Error("Failed to update container statuses", "error", err)
			return ctrl.Result{}, fmt.Errorf("updating container statuses: %w", err)
		}
		pod := snap.pod

		// A pod wedged in a terminal waiting state (e.g. ImagePullBackOff) never
		// runs, so its Job never gets a Complete/Failed condition and jobFinished
		// would keep the BuildJob "Running" forever. Fail it with the reason and
		// delete the Job (removing the runaway pod and stopping the endless pull
		// retries, which TTLSecondsAfterFinished would never reap on an unfinished
		// Job). No log-drain gate: the containers never started, so there are no
		// logs to wait for.
		if reason, stuck := podStuckReason(pod); stuck {
			slog.Warn("BuildJob pod is stuck; failing", "buildJob", bj.Name, "reason", reason)
			if _, err := r.setStatusCompleted(ctx, req.NamespacedName, bj.Name, false, reason); err != nil {
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
			if err := r.admitSuspendedJobs(ctx, bj.Namespace); err != nil {
				slog.Error("Failed to admit suspended job after stuck-pod failure", "error", err)
			}
			return ctrl.Result{}, nil
		}

		// Job already exists, check the status of the job
		done, succeeded, msg := jobFinished(&existingJob)
		if done {
			// Hold off finalizing (and deleting the pod) until every container's
			// log stream has drained, so no tail logs are lost. A cleanly finished
			// stream enqueues the BuildJob itself (see logStreamRegistry.events);
			// the requeue is the fallback for a dropped event or a failed stream.
			if snap.draining {
				return ctrl.Result{RequeueAfter: logDrainRequeueDelay}, nil
			}

			// Drain gate: when the CR (and thus the pod) will be deleted on
			// completion, all logs of terminated containers must be published
			// first, because Kubernetes only serves container logs while the pod
			// exists. This runs BEFORE setStatusCompleted on purpose: setting the
			// terminal phase would trip the early-return guard at the top of
			// Reconcile, so we keep the phase non-terminal and requeue until the
			// logs are drained (or a safety valve fires).
			//
			// The pod and statuses come from this reconcile's own status pass. A
			// pod that is already gone never gets here: resolving it fails and the
			// error branch above finalizes without draining.
			timedOut := time.Since(jobCompletionTime(&existingJob)) > r.logDrainTimeout()
			if wait, why := drainGate(pod, snap.statuses, r.DeleteOnComplete, timedOut); wait {
				slog.Info("Waiting before finalizing", "buildJob", bj.Name, "reason", why)
				return ctrl.Result{RequeueAfter: logDrainRequeueDelay}, nil
			} else if why != "" {
				slog.Warn("Finalizing before logs drained", "buildJob", bj.Name, "reason", why)
			}

			changed, err := r.setStatusCompleted(ctx, req.NamespacedName, bj.Name, succeeded, msg)
			if err != nil {
				if apierrors.IsConflict(err) {
					return ctrl.Result{RequeueAfter: conflictRequeueDelay}, nil
				}
				return ctrl.Result{}, err
			}

			// Emit the job's overhead/runtime breakdown, derived from the pod's
			// container timestamps. Only the reconcile that actually made the
			// terminal transition records it; a concurrent one working from a
			// stale cache sees changed=false.
			if changed {
				r.emitJobTiming(ctx, &bj, pod)
			}

			// All logs are published; stop tracking this job's streams.
			r.logStreams().stopJob(bj.Namespace, bj.Name)

			// If DeleteOnComplete is false, don't delete the CR, used for debugging
			if !r.DeleteOnComplete {
				// If there is available concurrency, admit one suspended job (optional)
				if err := r.admitSuspendedJobs(ctx, bj.Namespace); err != nil {
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
			if err := r.admitSuspendedJobs(ctx, bj.Namespace); err != nil {
				slog.Error("Failed to admit suspended job after deletion", "error", err)
			}
			return ctrl.Result{}, nil
		}

		// Build is not done, set the status to be "running"
		if bj.Status.Phase != string(buildstatus.StatusRunning) {
			if err := r.setStatusRunning(ctx, req.NamespacedName, bj.Name); err != nil {
				if apierrors.IsConflict(err) {
					return ctrl.Result{RequeueAfter: conflictRequeueDelay}, nil
				}
				return ctrl.Result{}, err
			}
		}

		// Container transitions arrive as pod events and finished log streams as
		// channel events, so this requeue is only the safety resync (see
		// DefaultRequeueDelay).
		return ctrl.Result{RequeueAfter: r.requeueDelay()}, nil
	}

	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	// ----------------------------- 3. First-time creation -----------------------------
	// 3.0-3.3 Check the concurrency limit and create the Job (initContainers =
	// bj.Spec.Steps), suspended if the limit is reached.
	active, shouldSuspend, err := r.createJob(ctx, &bj, jobName)
	if err != nil {
		return ctrl.Result{}, err
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
		return ctrl.Result{RequeueAfter: r.requeueDelay()}, nil
	}

	//3.4.2 Otherwise, set to Running
	if err := r.setStatusRunning(ctx, req.NamespacedName, bj.Name); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: conflictRequeueDelay}, nil
		}
		return ctrl.Result{}, err
	}

	// The pod's creation and container transitions arrive as pod events; this
	// is only the safety resync.
	return ctrl.Result{RequeueAfter: r.requeueDelay()}, nil
}

// createJob creates the BuildJob's Job, suspended when MaxParallelism Jobs are
// already active. The count and the create happen under the admission lock so
// concurrent reconciles cannot all claim the same free slot.
func (r *BuildJobReconciler) createJob(ctx context.Context, bj *buildv1.BuildJob, jobName string) (active uint, suspended bool, err error) {
	r.admission.mu.Lock()
	defer r.admission.mu.Unlock()

	jobs, err := r.listManagedJobs(ctx, bj.Namespace)
	if err != nil {
		return 0, false, err
	}
	active = r.admission.activeCount(bj.Namespace, jobs, time.Now())
	suspended = active >= r.MaxParallelism

	k8sJob := buildK8sJob(bj, jobName, r.DeleteOnComplete, suspended, r.finalizerImage(), r.ImagePullPolicy)
	if err := controllerutil.SetControllerReference(bj, k8sJob, r.Scheme); err != nil {
		return 0, false, err
	}

	slog.Info("Creating Job for BuildJob", "job", k8sJob.Name, "suspended", suspended)
	if err := r.Create(ctx, k8sJob); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			slog.Error("cannot create Job", "error", err)
			return 0, false, err
		}
		// The Job is left over from an earlier attempt whose status write
		// failed. Its suspend flag was decided back then, so report that one
		// rather than what this pass would have chosen.
		slog.Debug("Job already exists", "job", k8sJob.Name)
		var existing batchv1.Job
		if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(k8sJob), &existing); err != nil {
			return 0, false, err
		}
		return active, jobSuspended(&existing), nil
	}
	if !suspended {
		r.admission.markAdmitted(bj.Namespace, jobName, time.Now())
	}
	return active, suspended, nil
}

// setStatusRunning sets BuildJob.Status to "Running" and records StartTime.
// publishes "running" jobstatus to NATS.
// Uses optimistic concurrency (RetryOnConflict).
//
// PodName is deliberately not touched: it is owned by writeContainerStatuses,
// which records the real pod name once the pod exists.
//
// Like setStatusCompleted it reads uncached and patches with an optimistic
// lock, and publishes only when it made the transition itself, so two
// reconciles racing on a stale cache cannot both announce Running.
func (r *BuildJobReconciler) setStatusRunning(ctx context.Context, nn types.NamespacedName, jobID string) error {
	changed := false
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		changed = false
		latest := &buildv1.BuildJob{}
		if err := r.apiReader().Get(ctx, nn, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.DeletionTimestamp != nil {
			return nil
		}
		// Any phase past Pending means another reconcile got here first; a
		// terminal phase in particular must not regress to Running.
		if latest.Status.Phase != "" && latest.Status.Phase != phasePending {
			return nil
		}

		base := latest.DeepCopy()
		now := metav1.Now()
		latest.Status.Phase = string(buildstatus.StatusRunning)
		latest.Status.StartTime = &now

		if err := r.Status().Patch(ctx, latest, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			slog.Error("Failed to patch BuildJob status to Running", "error", err)
			return err
		}
		changed = true
		return nil
	}); err != nil {
		return err
	}
	if !changed {
		return nil
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
//
// changed reports whether this call made the terminal transition. The read is
// uncached and the patch carries the resourceVersion: a reconcile triggered by
// a pod event right after finalization still sees the cached BuildJob as
// Running, and without both it would publish the terminal status a second time.
func (r *BuildJobReconciler) setStatusCompleted(ctx context.Context, nn types.NamespacedName, jobID string, succeeded bool, msg string) (changed bool, err error) {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		changed = false
		latest := &buildv1.BuildJob{}
		if err := r.apiReader().Get(ctx, nn, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if latest.DeletionTimestamp != nil {
			return nil
		}

		if phaseFinal(latest.Status.Phase) {
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

		if err := r.Status().Patch(ctx, latest, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		changed = true
		return nil
	}); err != nil {
		return false, err
	}
	if !changed {
		return false, nil
	}

	if succeeded {
		if err := r.Publisher.PublishJobStatus(ctx, buildstatus.StatusSucceeded, jobID); err != nil {
			slog.Error("Failed to publish job succeeded status", "job_id", jobID, "error", err)
			return true, err
		}
		slog.Info("Published job succeeded status", "job_id", jobID)
		return true, nil
	}
	if err := r.Publisher.PublishJobStatus(ctx, buildstatus.StatusFailed, jobID, msg); err != nil {
		slog.Error("Failed to publish job failed status", "job_id", jobID, "error", err)
		return true, err
	}
	slog.Info("Published job failed status", "job_id", jobID)
	return true, nil
}

// drainGate decides whether a BuildJob whose Job has finished must wait before
// it is finalized. Finalizing cancels every log stream and, with
// deleteOnComplete, deletes the pod, so it waits for:
//   - the pod to reach a terminal phase. The Job's condition can be ahead of the
//     cached pod, which may still show a step as Running; that step does not
//     count as terminated below, and its stream would be cut before EOF.
//   - with deleteOnComplete, every terminated container's logs to be published,
//     because Kubernetes only serves container logs while the pod exists.
//
// timedOut (logDrainTimeout since the Job finished) overrides both; why then
// explains what was skipped, for the log.
func drainGate(pod *corev1.Pod, statuses []buildv1.ContainerStatus, deleteOnComplete, timedOut bool) (wait bool, why string) {
	if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
		why = fmt.Sprintf("pod phase is %q, not terminal yet", pod.Status.Phase)
	} else if deleteOnComplete && !allTerminatedLogsPublished(pod, statuses) {
		why = "terminated container logs not published yet"
	} else {
		return false, ""
	}
	if timedOut {
		return false, "timed out: " + why
	}
	return true, why
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
func buildK8sJob(bj *buildv1.BuildJob, jobName string, deleteOnComplete bool, suspend bool, finalizerImage string, pullPolicy corev1.PullPolicy) *batchv1.Job {
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
			Name:  fmt.Sprintf(BuildStepPrefix, s.ID),
			Image: s.Image,
			// Empty leaves the field unset so Kubernetes applies its own default
			// (Always for :latest or untagged images, IfNotPresent otherwise).
			ImagePullPolicy: pullPolicy,
			// Match the Docker executor, which sets the working directory to the
			// shared volume for every step (HadesScheduler/docker/step.go).
			// Without this the container starts in whatever WORKDIR its image
			// declares, so an identical job document resolves relative paths
			// differently on the two executors: with alpine (WORKDIR /) a step
			// ran in / here and in /shared under Docker, and with an image
			// declaring WORKDIR /app it would run in /app. That silently breaks
			// the promise that the same job runs unmodified on either executor.
			WorkingDir:   sharedMount.MountPath,
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

	// Add a dummy container that runs after all init containers have finished.
	//
	// The tag and the pull policy are both explicit, and both matter. An
	// untagged image means ":latest", and Kubernetes forces
	// imagePullPolicy: Always for ":latest" - so every single build job made a
	// registry round trip for this container even though the image was already
	// on the node. It is the same defect the Docker executor had before #512,
	// where a cached pull cost ~2.2s per step; here it was once per job, on the
	// critical path, for a container that echoes one line.
	dummy := corev1.Container{
		Name:            FinalizerContainerName,
		Image:           finalizerImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"sh", "-c", "echo build finished"},
		VolumeMounts:    []corev1.VolumeMount{sharedMount},
	}

	gracePeriod := int64(podTerminationGracePeriodSeconds)
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
		// Step scripts run as `sh -c` PID 1, which ignores SIGTERM, so on a
		// deadline or a delete the kubelet would otherwise sit out the default
		// 30s grace period before killing the pod.
		TerminationGracePeriodSeconds: &gracePeriod,
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
				// The operator's pod informer is filtered on LabelManagedBy and its
				// pod watch maps LabelBuildJob back to the BuildJob, so the pod
				// needs the same labels as the Job.
				ObjectMeta: metav1.ObjectMeta{Labels: maps.Clone(labels)},
				Spec:       podSpec,
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

// SetupWithManager registers the controller. Besides BuildJobs and the Jobs it
// owns, it watches the build pods (container transitions drive log streaming and
// completion) and the log-stream registry's finished-stream events, so a running
// job progresses on events instead of on the RequeueDelay poll.
func (r *BuildJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&buildv1.BuildJob{}).
		Owns(&batchv1.Job{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(podToBuildJob)).
		WatchesRawSource(source.Channel(r.logStreams().Events(), &handler.EnqueueRequestForObject{})).
		WithOptions(ctrlcontroller.Options{MaxConcurrentReconciles: r.maxConcurrentReconciles()}).
		Named("buildjob").
		Complete(r)
}

// podToBuildJob maps a build pod to its BuildJob. Pods are owned by the Job, not
// the BuildJob, so the owner-reference handlers cannot do this; the pod
// template carries LabelBuildJob instead (see buildK8sJob). Pods created before
// that label existed are not mapped; the resync requeue still covers them.
func podToBuildJob(_ context.Context, obj client.Object) []reconcile.Request {
	labels := obj.GetLabels()
	if labels[LabelManagedBy] != ManagedByValue {
		return nil
	}
	name := labels[LabelBuildJob]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}}}
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

// listManagedJobs lists the operator's Jobs in namespace for admission. It reads
// uncached: the pending tracker only covers this process's own admissions, so a
// cache that has not caught up with everything else would still undercount.
// When it does fall back to the cache, the items are the cache's own objects
// (no deep copy), so callers must copy before mutating.
func (r *BuildJobReconciler) listManagedJobs(ctx context.Context, namespace string) ([]batchv1.Job, error) {
	var jl batchv1.JobList
	if err := r.apiReader().List(ctx, &jl,
		client.InNamespace(namespace),
		client.MatchingLabels{LabelManagedBy: ManagedByValue},
		client.UnsafeDisableDeepCopy,
	); err != nil {
		return nil, err
	}
	return jl.Items, nil
}

// selfAdmitSuspendedJobs is admitSuspendedJobs for a suspended BuildJob's own
// reconcile, coalesced to one pass per namespace per selfAdmitInterval.
func (r *BuildJobReconciler) selfAdmitSuspendedJobs(ctx context.Context, namespace string) error {
	return r.admitSuspendedJobsEvery(ctx, namespace, selfAdmitInterval)
}

// admitSuspendedJobs unsuspends suspended Jobs in the given namespace, highest
// priority (then oldest) first, until MaxParallelism Jobs are active. It runs
// under the admission lock and skips Jobs it already admitted whose new state
// the cache does not show yet, so concurrent reconciles neither exceed the limit
// nor admit the same Job twice.
func (r *BuildJobReconciler) admitSuspendedJobs(ctx context.Context, namespace string) error {
	return r.admitSuspendedJobsEvery(ctx, namespace, 0)
}

// admitSuspendedJobsEvery skips the pass if the namespace had one less than
// minInterval ago; 0 always runs it.
func (r *BuildJobReconciler) admitSuspendedJobsEvery(ctx context.Context, namespace string, minInterval time.Duration) error {
	r.admission.mu.Lock()
	defer r.admission.mu.Unlock()

	now := time.Now()
	if minInterval > 0 && now.Sub(r.admission.lastPass[namespace]) < minInterval {
		return nil
	}
	if r.admission.lastPass == nil {
		r.admission.lastPass = make(map[string]time.Time)
	}
	r.admission.lastPass[namespace] = now

	jobs, err := r.listManagedJobs(ctx, namespace)
	if err != nil {
		return err
	}
	active := r.admission.activeCount(namespace, jobs, now)
	if active >= r.MaxParallelism {
		return nil
	}

	// Priority comes from the Job's own label only. buildK8sJob copies it from
	// the BuildJob when the Job is created, so looking the BuildJob up as well
	// would cost a Get per candidate per pass to learn the same value.
	type candidate struct {
		job      *batchv1.Job
		priority int
	}
	candidates := make([]candidate, 0)
	for i := range jobs {
		j := &jobs[i]
		if done, _, _ := jobFinished(j); done || !jobSuspended(j) {
			continue
		}
		if r.admission.isPending(j.Namespace, j.Name) {
			continue
		}
		candidates = append(candidates, candidate{j, priorityFromLabels(j.Labels, defaultPriority)})
	}
	sort.SliceStable(candidates, func(a, b int) bool {
		ca, cb := candidates[a], candidates[b]
		if ca.priority != cb.priority {
			return ca.priority > cb.priority
		}
		return ca.job.CreationTimestamp.Before(&cb.job.CreationTimestamp)
	})

	for _, c := range candidates {
		if active >= r.MaxParallelism {
			break
		}
		// c.job is the cache's object (see listManagedJobs); patch a copy.
		admitted := c.job.DeepCopy()
		f := false
		admitted.Spec.Suspend = &f

		slog.Info("Admitting suspended Job", "job", admitted.Name, "priority", c.priority)
		if err := r.Patch(ctx, admitted, client.MergeFrom(c.job)); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		r.admission.markAdmitted(admitted.Namespace, admitted.Name, now)
		active++
	}
	return nil
}

func jobSuspended(j *batchv1.Job) bool {
	return j.Spec.Suspend != nil && *j.Spec.Suspend
}

// phaseFinal reports whether a BuildJob phase is one the operator writes as its
// last. Not buildstatus.IsTerminal: that also counts Stopped, which the operator
// never sets.
func phaseFinal(phase string) bool {
	return phase == string(buildstatus.StatusSucceeded) || phase == string(buildstatus.StatusFailed)
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
