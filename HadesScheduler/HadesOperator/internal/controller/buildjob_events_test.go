package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	"github.com/hades-scheduler/hades/hadesScheduler/k8s"
	"github.com/hades-scheduler/hades/hadesScheduler/log"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBuildK8sJob_PodTemplateLabelsAndGracePeriod(t *testing.T) {
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1", Labels: map[string]string{LabelPriority: "3"}},
		Spec:       buildv1.BuildJobSpec{Steps: []buildv1.BuildStep{{ID: 1, Image: "alpine"}}},
	}
	job := buildK8sJob(bj, "buildjob-job-1", true, false, DefaultFinalizerImage, "")

	podLabels := job.Spec.Template.Labels
	if podLabels[LabelManagedBy] != ManagedByValue || podLabels[LabelBuildJob] != "job-1" || podLabels[LabelPriority] != "3" {
		t.Errorf("pod template labels = %v, want managed-by, buildjob and priority", podLabels)
	}
	// The template must not share the Job's map, or mutating one would change both.
	podLabels["x"] = "y"
	if _, ok := job.Labels["x"]; ok {
		t.Error("pod template labels alias the Job labels")
	}

	gp := job.Spec.Template.Spec.TerminationGracePeriodSeconds
	if gp == nil || *gp != podTerminationGracePeriodSeconds {
		t.Errorf("TerminationGracePeriodSeconds = %v, want %d", gp, podTerminationGracePeriodSeconds)
	}
}

func TestPodToBuildJob(t *testing.T) {
	pod := func(labels map[string]string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", Labels: labels}}
	}

	reqs := podToBuildJob(context.Background(), pod(map[string]string{LabelManagedBy: ManagedByValue, LabelBuildJob: "job-1"}))
	if len(reqs) != 1 || reqs[0].Namespace != "ns" || reqs[0].Name != "job-1" {
		t.Errorf("labelled pod mapped to %v, want ns/job-1", reqs)
	}
	if reqs := podToBuildJob(context.Background(), pod(map[string]string{LabelBuildJob: "job-1"})); len(reqs) != 0 {
		t.Errorf("pod without managed-by label mapped to %v, want none", reqs)
	}
	if reqs := podToBuildJob(context.Background(), pod(map[string]string{LabelManagedBy: ManagedByValue})); len(reqs) != 0 {
		t.Errorf("pod without buildjob label mapped to %v, want none", reqs)
	}
}

func waitStreamFinished(t *testing.T, s *logStream) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if finished, _ := s.result(); finished {
			return
		}
		select {
		case <-deadline:
			t.Fatal("stream did not finish")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// A cleanly finished stream must enqueue its BuildJob; a failed one must not, so a
// stream that keeps failing cannot hot-loop the controller.
func TestLogStreamRegistry_NotifiesOnCleanFinishOnly(t *testing.T) {
	reg := NewLogStreamRegistry()

	reg.ensureStarted(logStreamKey("ns", "job-1", "step-1"), func(context.Context, func(time.Time)) error { return nil })
	select {
	case ev := <-reg.Events():
		if ev.Object.GetNamespace() != "ns" || ev.Object.GetName() != "job-1" {
			t.Errorf("event for %s/%s, want ns/job-1", ev.Object.GetNamespace(), ev.Object.GetName())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event after a clean stream finish")
	}

	failKey := logStreamKey("ns", "job-2", "step-1")
	reg.ensureStarted(failKey, func(context.Context, func(time.Time)) error { return errors.New("boom") })
	waitStreamFinished(t, reg.get(failKey))
	select {
	case ev := <-reg.Events():
		t.Errorf("unexpected event for failed stream: %s", ev.Object.GetName())
	case <-time.After(50 * time.Millisecond):
	}
}

// With nobody draining the channel, finishing streams must still exit.
func TestLogStreamRegistry_NotifyNeverBlocks(t *testing.T) {
	reg := NewLogStreamRegistry()
	for i := 0; i < logStreamEventBuffer+5; i++ {
		reg.notifyFinished(logStreamKey("ns", "job", "c"))
	}
	if got := len(reg.events); got != logStreamEventBuffer {
		t.Errorf("buffered %d events, want %d", got, logStreamEventBuffer)
	}

	// A registry built as a zero value (no channel) must not panic either.
	(&logStreamRegistry{}).notifyFinished(logStreamKey("ns", "job", "c"))
}

// The finished-stream event usually beats the pod status update, so a cleanly
// ended stream of a container the pod still reports as running must not be
// restarted (that would republish its logs).
func TestUpdateContainerStateMap_KeepsCleanlyEndedStreamDuringGrace(t *testing.T) {
	r := &BuildJobReconciler{LogStreams: NewLogStreamRegistry()}
	bj := &buildv1.BuildJob{ObjectMeta: metav1.ObjectMeta{Name: "job-1", Namespace: "ns"}}
	key := logStreamKey("ns", "job-1", "step-1")
	r.LogStreams.ensureStarted(key, func(context.Context, func(time.Time)) error { return nil })
	stream := r.LogStreams.get(key)
	waitStreamFinished(t, stream)

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1"}}
	statusMap := map[string]buildv1.ContainerStatus{"step-1": {Name: "step-1", StepID: 1}}
	cs := r.updateContainerStateMap(context.Background(), bj, pod, statusMap, running("step-1"))

	if r.LogStreams.get(key) != stream {
		t.Fatal("cleanly ended stream was replaced while the pod status still says running")
	}
	if cs.LogsPublished {
		t.Fatal("logs marked published before the container terminated")
	}

	// Once the pod catches up, the same stream marks the container published.
	cs = r.updateContainerStateMap(context.Background(), bj, pod, statusMap, terminated("step-1"))
	if !cs.LogsPublished {
		t.Fatal("terminated container with a cleanly finished stream not marked published")
	}
}

func suspendedJob(name string, created time.Time, priority string) batchv1.Job {
	suspend := true
	return batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "ns",
			CreationTimestamp: metav1.NewTime(created),
			Labels:            map[string]string{LabelManagedBy: ManagedByValue, LabelPriority: priority},
		},
		Spec: batchv1.JobSpec{Suspend: &suspend},
	}
}

func newAdmissionTestReconciler(t *testing.T, maxParallelism uint, objs ...client.Object) *BuildJobReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := buildv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &BuildJobReconciler{Client: c, Scheme: scheme, MaxParallelism: maxParallelism}
}

func isSuspended(t *testing.T, r *BuildJobReconciler, name string) bool {
	t.Helper()
	var j batchv1.Job
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: name}, &j); err != nil {
		t.Fatal(err)
	}
	return jobSuspended(&j)
}

// admitSuspendedJobs fills free slots in priority order and never past the limit.
// The priority comes from the Job label alone (no BuildJob lookups).
func TestAdmitSuspendedJobs_FillsFreeSlotsByPriority(t *testing.T) {
	now := time.Now()
	runningJob := suspendedJob("running", now, "1")
	runningJob.Spec.Suspend = nil
	low := suspendedJob("low", now.Add(-time.Hour), "1")
	high := suspendedJob("high", now, "3")
	mid := suspendedJob("mid", now, "2")

	r := newAdmissionTestReconciler(t, 3, &runningJob, &low, &high, &mid)
	if err := r.admitSuspendedJobs(context.Background(), "ns"); err != nil {
		t.Fatal(err)
	}
	if isSuspended(t, r, "high") || isSuspended(t, r, "mid") {
		t.Error("the two highest-priority jobs should have been admitted")
	}
	if !isSuspended(t, r, "low") {
		t.Error("low was admitted beyond MaxParallelism")
	}

	// At capacity: a second call (e.g. from another worker) admits nothing.
	if err := r.admitSuspendedJobs(context.Background(), "ns"); err != nil {
		t.Fatal(err)
	}
	if !isSuspended(t, r, "low") {
		t.Error("low was admitted on a repeat call at capacity")
	}
}

// An admission the cache does not show yet must still occupy its slot, or a
// concurrent reconcile reading the stale cache would admit past the limit.
func TestAdmissionTracker_CountsUnobservedAdmissions(t *testing.T) {
	now := time.Now()
	var a admissionTracker
	stale := suspendedJob("a", now, "1") // admitted, but cache still says suspended
	a.markAdmitted("ns", "a", now)
	a.markAdmitted("ns", "b", now)                   // created, not in the cache at all
	a.markAdmitted("other", "c", now)                // other namespace
	a.markAdmitted("ns", "old", now.Add(-time.Hour)) // never observed; expired

	if got := a.activeCount("ns", []batchv1.Job{stale}, now); got != 2 {
		t.Errorf("activeCount = %d, want 2 (a and b)", got)
	}
	if a.isPending("ns", "old") {
		t.Error("expired pending admission was not forgotten")
	}

	// Once the cache shows the Job running, it is counted from the list and the
	// pending entry is dropped.
	observed := suspendedJob("a", now, "1")
	observed.Spec.Suspend = nil
	if got := a.activeCount("ns", []batchv1.Job{observed}, now); got != 2 {
		t.Errorf("activeCount = %d, want 2 (a from cache, b pending)", got)
	}
	if a.isPending("ns", "a") {
		t.Error("pending admission kept after the cache caught up")
	}
}

func TestResolvePod(t *testing.T) {
	ctx := context.Background()
	bj := &buildv1.BuildJob{ObjectMeta: metav1.ObjectMeta{Name: "job-1", Namespace: "ns"}}

	t.Run("labelled template reads the pod from the cached client", func(t *testing.T) {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "buildjob-job-1-abc", Namespace: "ns",
			Labels: map[string]string{LabelManagedBy: ManagedByValue, LabelBuildJob: "job-1"},
		}}
		other := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "buildjob-job-2-abc", Namespace: "ns",
			Labels: map[string]string{LabelManagedBy: ManagedByValue, LabelBuildJob: "job-2"},
		}}
		r := newAdmissionTestReconciler(t, 1, pod, other)
		job := buildK8sJob(bj, "buildjob-job-1", true, false, DefaultFinalizerImage, "")

		got, err := r.resolvePod(ctx, bj, job)
		if err != nil || got.Name != pod.Name {
			t.Fatalf("resolvePod() = %v, %v; want %s", got, err, pod.Name)
		}
	})

	t.Run("no pod yet is ErrPodNotFound", func(t *testing.T) {
		r := newAdmissionTestReconciler(t, 1)
		job := buildK8sJob(bj, "buildjob-job-1", true, false, DefaultFinalizerImage, "")
		if _, err := r.resolvePod(ctx, bj, job); !errors.Is(err, k8s.ErrPodNotFound) {
			t.Fatalf("resolvePod() error = %v, want ErrPodNotFound", err)
		}
	})

	t.Run("unlabelled template of an older Job falls back to the API", func(t *testing.T) {
		legacy := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "buildjob-job-1-old", Namespace: "ns",
			Labels: map[string]string{"job-name": "buildjob-job-1"},
		}}
		r := newAdmissionTestReconciler(t, 1)
		r.K8sClient = k8sfake.NewClientset(legacy)
		r.Publisher = &log.NATSPublisher{}
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "buildjob-job-1", Namespace: "ns"}}

		got, err := r.resolvePod(ctx, bj, job)
		if err != nil || got.Name != legacy.Name {
			t.Fatalf("resolvePod() = %v, %v; want %s", got, err, legacy.Name)
		}
	})
}

// A pass working from a stale BuildJob (LogsPublished still false) must not
// follow a container again whose stream already finished and was published: the
// finished entry stays in the registry as the marker until stopJob.
func TestUpdateContainerStateMap_PublishedStreamIsNotRestartedFromStaleStatus(t *testing.T) {
	r := &BuildJobReconciler{LogStreams: NewLogStreamRegistry()}
	bj := &buildv1.BuildJob{ObjectMeta: metav1.ObjectMeta{Name: "job-1", Namespace: "ns"}}
	key := logStreamKey("ns", "job-1", "step-1")
	r.LogStreams.ensureStarted(key, func(context.Context, func(time.Time)) error { return nil })
	stream := r.LogStreams.get(key)
	waitStreamFinished(t, stream)

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1"}}
	stale := map[string]buildv1.ContainerStatus{"step-1": {Name: "step-1", StepID: 1}}

	// r.Publisher is nil, so starting a new stream (which registers a log slot)
	// would panic: reaching the assertions proves no restart happened.
	for i := 0; i < 2; i++ {
		cs := r.updateContainerStateMap(context.Background(), bj, pod, stale, terminated("step-1"))
		if !cs.LogsPublished {
			t.Fatalf("pass %d: not marked published", i)
		}
		if r.LogStreams.get(key) != stream {
			t.Fatalf("pass %d: published stream was dropped or replaced", i)
		}
	}

	r.LogStreams.stopJob("ns", "job-1")
	if r.LogStreams.get(key) != nil {
		t.Fatal("stopJob did not forget the published marker")
	}
}

// When the cached BuildJob is stale but the live one is already terminal, the
// status pass must bail out instead of restarting streams.
func TestUpdateContainerStatuses_StopsOnLiveFinalizedBuildJob(t *testing.T) {
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1", Namespace: "ns"},
		Status:     buildv1.BuildJobStatus{Phase: "Succeeded"},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "buildjob-job-1-abc", Namespace: "ns",
		Labels: map[string]string{LabelManagedBy: ManagedByValue, LabelBuildJob: "job-1"},
	}}
	r := newAdmissionTestReconciler(t, 1, bj, pod)
	job := buildK8sJob(bj, "buildjob-job-1", true, false, DefaultFinalizerImage, "")

	stale := bj.DeepCopy()
	stale.Status.Phase = "Running"
	if _, err := r.updateContainerStatuses(context.Background(), stale, job); !errors.Is(err, errBuildJobFinalized) {
		t.Fatalf("updateContainerStatuses() error = %v, want errBuildJobFinalized", err)
	}
}

// A reconcile on a stale cache must not publish (or record timing for) a
// terminal or running transition another reconcile already made. r.Publisher is
// nil, so any publish attempt would panic.
func TestStatusTransitions_DoNotRepublish(t *testing.T) {
	ctx := context.Background()
	done := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: "ns"},
		Status:     buildv1.BuildJobStatus{Phase: "Failed"},
	}
	running := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "running", Namespace: "ns"},
		Status:     buildv1.BuildJobStatus{Phase: "Running"},
	}
	r := newAdmissionTestReconciler(t, 1, done, running)

	changed, err := r.setStatusCompleted(ctx, client.ObjectKeyFromObject(done), "done", true, "")
	if err != nil || changed {
		t.Fatalf("setStatusCompleted() on terminal BuildJob = %v, %v; want false, nil", changed, err)
	}
	if err := r.setStatusRunning(ctx, client.ObjectKeyFromObject(running), "running"); err != nil {
		t.Fatalf("setStatusRunning() on running BuildJob: %v", err)
	}
	if err := r.setStatusRunning(ctx, client.ObjectKeyFromObject(done), "done"); err != nil {
		t.Fatalf("setStatusRunning() on terminal BuildJob: %v", err)
	}
	var got buildv1.BuildJob
	if err := r.Get(ctx, client.ObjectKeyFromObject(done), &got); err != nil || got.Status.Phase != "Failed" {
		t.Fatalf("terminal phase regressed to %q (err %v)", got.Status.Phase, err)
	}
}

func TestDrainGate(t *testing.T) {
	podIn := func(phase corev1.PodPhase, init ...corev1.ContainerStatus) *corev1.Pod {
		return &corev1.Pod{Status: corev1.PodStatus{Phase: phase, InitContainerStatuses: init}}
	}
	published := []buildv1.ContainerStatus{status("step-1", true)}
	unpublished := []buildv1.ContainerStatus{status("step-1", false)}

	tests := []struct {
		name             string
		pod              *corev1.Pod
		statuses         []buildv1.ContainerStatus
		deleteOnComplete bool
		timedOut         bool
		wantWait         bool
	}{
		{"cached pod still running holds even without delete", podIn(corev1.PodRunning, running("step-1")), unpublished, false, false, true},
		{"cached pod still running holds with delete", podIn(corev1.PodRunning, running("step-1")), unpublished, true, false, true},
		{"terminal pod, logs published", podIn(corev1.PodFailed, terminated("step-1")), published, true, false, false},
		{"terminal pod, logs pending, delete", podIn(corev1.PodFailed, terminated("step-1")), unpublished, true, false, true},
		{"terminal pod, logs pending, no delete", podIn(corev1.PodSucceeded, terminated("step-1")), unpublished, false, false, false},
		{"timeout overrides a running pod", podIn(corev1.PodRunning, running("step-1")), unpublished, true, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wait, why := drainGate(tt.pod, tt.statuses, tt.deleteOnComplete, tt.timedOut)
			if wait != tt.wantWait {
				t.Errorf("drainGate() wait = %v (%s), want %v", wait, why, tt.wantWait)
			}
			if tt.timedOut && why == "" {
				t.Error("timed-out finalization should explain what it skipped")
			}
		})
	}
}

// Suspended BuildJobs' own admission passes coalesce; completions always admit.
func TestSelfAdmitSuspendedJobs_Coalesces(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	s1 := suspendedJob("s1", now, "1")
	r := newAdmissionTestReconciler(t, 5, &s1)

	if err := r.selfAdmitSuspendedJobs(ctx, "ns"); err != nil {
		t.Fatal(err)
	}
	if isSuspended(t, r, "s1") {
		t.Fatal("first self-admission pass did not admit s1")
	}

	s2 := suspendedJob("s2", now, "1")
	if err := r.Create(ctx, &s2); err != nil {
		t.Fatal(err)
	}
	if err := r.selfAdmitSuspendedJobs(ctx, "ns"); err != nil {
		t.Fatal(err)
	}
	if !isSuspended(t, r, "s2") {
		t.Fatal("second self-admission pass within selfAdmitInterval was not coalesced")
	}

	if err := r.admitSuspendedJobs(ctx, "ns"); err != nil {
		t.Fatal(err)
	}
	if isSuspended(t, r, "s2") {
		t.Fatal("completion-driven admission must not be coalesced")
	}
}

// A clean EOF past the grace period must not restart the stream when the live
// pod shows the container already stopped (kubelet status lagging the cache).
func TestUpdateContainerStateMap_LivePodVetoesRestartAfterGrace(t *testing.T) {
	livePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns"},
		Status:     corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{terminated("step-1")}},
	}
	r := newAdmissionTestReconciler(t, 1, livePod)
	r.LogStreams = NewLogStreamRegistry()
	bj := &buildv1.BuildJob{ObjectMeta: metav1.ObjectMeta{Name: "job-1", Namespace: "ns"}}
	key := logStreamKey("ns", "job-1", "step-1")
	r.LogStreams.ensureStarted(key, func(context.Context, func(time.Time)) error { return nil })
	stream := r.LogStreams.get(key)
	waitStreamFinished(t, stream)
	stream.mu.Lock()
	stream.finishedAt = time.Now().Add(-2 * streamEOFGrace)
	stream.mu.Unlock()

	cachedPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns"}}
	statusMap := map[string]buildv1.ContainerStatus{"step-1": {Name: "step-1", StepID: 1}}
	r.updateContainerStateMap(context.Background(), bj, cachedPod, statusMap, running("step-1"))

	if r.LogStreams.get(key) != stream {
		t.Fatal("stream restarted although the live pod shows the container stopped")
	}
}

// When the Job already exists, createJob must report its real suspend flag,
// not the one this pass computed.
func TestCreateJob_AlreadyExistsReportsExistingSuspend(t *testing.T) {
	bj := &buildv1.BuildJob{ObjectMeta: metav1.ObjectMeta{Name: "job-1", Namespace: "ns", UID: "uid-1"}}
	existing := buildK8sJob(bj, "buildjob-job-1", true, false, DefaultFinalizerImage, "")
	// MaxParallelism 0 makes this pass want to suspend; the existing Job runs.
	r := newAdmissionTestReconciler(t, 0, bj, existing)

	_, suspended, err := r.createJob(context.Background(), bj, "buildjob-job-1")
	if err != nil {
		t.Fatal(err)
	}
	if suspended {
		t.Fatal("createJob reported suspended for an existing running Job")
	}
}
