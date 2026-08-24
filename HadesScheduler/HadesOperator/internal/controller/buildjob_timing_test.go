package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	"github.com/hades-scheduler/hades/shared/timing"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// phaseTracer records the phase and duration of each span for assertions.
type phaseTracer struct {
	mu  sync.Mutex
	got []phaseObservation
}

type phaseObservation struct {
	phase timing.Phase
	dur   time.Duration
}

func (p *phaseTracer) StartJob(ctx context.Context, _, _ string, _ time.Time) (context.Context, func()) {
	return ctx, func() {}
}

func (p *phaseTracer) Phase(_ context.Context, phase timing.Phase, start, end time.Time) {
	p.mu.Lock()
	p.got = append(p.got, phaseObservation{phase: phase, dur: end.Sub(start)})
	p.mu.Unlock()
}

func (p *phaseTracer) durations(phase timing.Phase) []time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []time.Duration
	for _, o := range p.got {
		if o.phase == phase {
			out = append(out, o.dur)
		}
	}
	return out
}

func terminatedAt(started, finished time.Time) corev1.ContainerState {
	return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		StartedAt:  metav1.NewTime(started),
		FinishedAt: metav1.NewTime(finished),
	}}
}

func TestRecordStepTiming(t *testing.T) {
	rec := &phaseTracer{}
	timing.SetTracer(rec)
	t.Cleanup(func() { timing.SetTracer(nil) })

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "job-1",
			CreationTimestamp: metav1.NewTime(base),
		},
		Spec: buildv1.BuildJobSpec{Steps: []buildv1.BuildStep{{ID: 1}, {ID: 2}}},
	}

	// Step 1: starts +2s, runs 3s. Step 2: starts +7s (2s wait after step 1), runs 2s.
	initStatuses := []corev1.ContainerStatus{
		{Name: "step-1", State: terminatedAt(base.Add(2*time.Second), base.Add(5*time.Second))},
		{Name: "step-2", State: terminatedAt(base.Add(7*time.Second), base.Add(9*time.Second))},
	}

	timer := timing.NewJobTimer(context.Background(), "k8s", bj.Name)
	recordStepTiming(timer, bj, initStatuses)

	if got := rec.durations(timing.PhaseProvision); len(got) != 1 || got[0] != 2*time.Second {
		t.Errorf("provision = %v, want [2s]", got)
	}
	if got := rec.durations(timing.PhaseStepWait); len(got) != 1 || got[0] != 2*time.Second {
		t.Errorf("step_wait = %v, want [2s]", got)
	}
	runs := rec.durations(timing.PhaseStepRun)
	if len(runs) != 2 || runs[0] != 3*time.Second || runs[1] != 2*time.Second {
		t.Errorf("step_run = %v, want [3s 2s]", runs)
	}
	if got := rec.durations(timing.PhaseReconcileLag); len(got) != 1 {
		t.Errorf("reconcile_detection_lag observations = %d, want 1", len(got))
	}
}

// TestRecordStepTimingUnorderedStatuses asserts the per-step math is correct even
// when initContainerStatuses is not in spec order (Kubernetes does not guarantee
// the order), because statuses are matched to steps by container name.
func TestRecordStepTimingUnorderedStatuses(t *testing.T) {
	rec := &phaseTracer{}
	timing.SetTracer(rec)
	t.Cleanup(func() { timing.SetTracer(nil) })

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-3", CreationTimestamp: metav1.NewTime(base)},
		Spec:       buildv1.BuildJobSpec{Steps: []buildv1.BuildStep{{ID: 1}, {ID: 2}}},
	}
	// Same timings as TestRecordStepTiming but statuses supplied in reverse order.
	initStatuses := []corev1.ContainerStatus{
		{Name: "step-2", State: terminatedAt(base.Add(7*time.Second), base.Add(9*time.Second))},
		{Name: "step-1", State: terminatedAt(base.Add(2*time.Second), base.Add(5*time.Second))},
	}

	timer := timing.NewJobTimer(context.Background(), "k8s", bj.Name)
	recordStepTiming(timer, bj, initStatuses)

	if got := rec.durations(timing.PhaseProvision); len(got) != 1 || got[0] != 2*time.Second {
		t.Errorf("provision = %v, want [2s]", got)
	}
	if got := rec.durations(timing.PhaseStepWait); len(got) != 1 || got[0] != 2*time.Second {
		t.Errorf("step_wait = %v, want [2s]", got)
	}
	// step_run must still be [step-1=3s, step-2=2s] in spec order despite reversed input.
	runs := rec.durations(timing.PhaseStepRun)
	if len(runs) != 2 || runs[0] != 3*time.Second || runs[1] != 2*time.Second {
		t.Errorf("step_run = %v, want [3s 2s]", runs)
	}
}

// TestRecordStepTimingSkipsUnrunSteps asserts a step that never ran (no
// Terminated state, e.g. after an earlier failure aborted the pod) contributes
// no runtime and does not break the wait chain.
func TestRecordStepTimingSkipsUnrunSteps(t *testing.T) {
	rec := &phaseTracer{}
	timing.SetTracer(rec)
	t.Cleanup(func() { timing.SetTracer(nil) })

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-2", CreationTimestamp: metav1.NewTime(base)},
		Spec:       buildv1.BuildJobSpec{Steps: []buildv1.BuildStep{{ID: 1}, {ID: 2}}},
	}
	initStatuses := []corev1.ContainerStatus{
		{Name: "step-1", State: terminatedAt(base.Add(1*time.Second), base.Add(2*time.Second))},
		{Name: "step-2", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}},
	}

	timer := timing.NewJobTimer(context.Background(), "k8s", bj.Name)
	recordStepTiming(timer, bj, initStatuses)

	if got := rec.durations(timing.PhaseStepRun); len(got) != 1 || got[0] != 1*time.Second {
		t.Errorf("step_run = %v, want [1s] (unrun step skipped)", got)
	}
	if got := rec.durations(timing.PhaseStepWait); len(got) != 0 {
		t.Errorf("step_wait = %v, want none", got)
	}
}
