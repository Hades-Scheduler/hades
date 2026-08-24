package timing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPhaseKind(t *testing.T) {
	runtime := map[Phase]bool{PhaseContainerRun: true, PhaseStepRun: true}
	all := []Phase{
		PhaseQueueWait, PhaseProvision, PhaseTeardown,
		PhaseImagePull, PhaseContainerCreate, PhaseContainerStartup, PhaseContainerRun,
		PhaseLogDrain, PhaseContainerRemove,
		PhaseStepWait, PhaseStepRun, PhaseReconcileLag,
	}
	for _, p := range all {
		want := KindOverhead
		if runtime[p] {
			want = KindRuntime
		}
		if got := p.Kind(); got != want {
			t.Errorf("%s.Kind() = %s, want %s", p, got, want)
		}
	}
}

func TestJobTimerRollup(t *testing.T) {
	timer := NewJobTimer(context.Background(), "docker", "job-1")
	base := time.Unix(1000, 0)

	// 100ms overhead + 400ms runtime + 100ms overhead => overhead 200ms, runtime 400ms.
	timer.Record(0, PhaseProvision, base, base.Add(100*time.Millisecond))
	timer.Record(1, PhaseContainerRun, base, base.Add(400*time.Millisecond))
	timer.Record(0, PhaseTeardown, base, base.Add(100*time.Millisecond))

	overhead, runtime := timer.totals()
	if overhead != 200*time.Millisecond {
		t.Errorf("overhead = %s, want 200ms", overhead)
	}
	if runtime != 400*time.Millisecond {
		t.Errorf("runtime = %s, want 400ms", runtime)
	}
	// overhead_pct = 200 / 600 = 33.3%. Summary must not panic and ends the span.
	timer.Summary()
}

func TestSummaryIsIdempotent(t *testing.T) {
	rec := &endCountingTracer{}
	SetTracer(rec)
	defer SetTracer(nil)

	timer := NewJobTimer(context.Background(), "docker", "job-sum")
	timer.Summary()
	timer.Summary() // second call (e.g. explicit + deferred) must be a no-op
	if n := rec.ends(); n != 1 {
		t.Errorf("root span ended %d times, want 1", n)
	}
}

func TestRecordClampsNegativeDuration(t *testing.T) {
	timer := NewJobTimer(context.Background(), "docker", "job-2")
	base := time.Unix(2000, 0)
	// end before start (clock skew) must clamp to zero, not subtract.
	timer.Record(1, PhaseStepRun, base, base.Add(-5*time.Second))
	_, runtime := timer.totals()
	if runtime != 0 {
		t.Errorf("runtime = %s, want 0 after clamp", runtime)
	}
}

func TestTimeRecordsOnError(t *testing.T) {
	wantErr := errors.New("boom")
	rec := &recordingTracer{}
	SetTracer(rec)
	defer SetTracer(noopTracer{})

	timer := NewJobTimer(context.Background(), "docker", "job-3")
	err := timer.Time(1, PhaseImagePull, func() error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("Time returned %v, want %v", err, wantErr)
	}
	if got := rec.phases(); len(got) != 1 || got[0] != PhaseImagePull {
		t.Errorf("recorded phases = %v, want [image_pull]", got)
	}
}

func TestInjectExtractRoundTrip(t *testing.T) {
	// With tracing disabled (noop), there is no span so Inject yields nothing and
	// Extract is a passthrough. This exercises the propagation plumbing without a
	// live exporter.
	carrier := Inject(context.Background())
	if carrier != nil {
		t.Errorf("Inject with no active span = %v, want nil", carrier)
	}
	ctx := Extract(context.Background(), map[string]string{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"})
	if ctx == nil {
		t.Error("Extract returned nil context")
	}
}

func TestInitTracingNoopWhenUnset(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err := InitTracing(context.Background(), "test")
	if err != nil {
		t.Fatalf("InitTracing error: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("noop shutdown error: %v", err)
	}
}

// recordingTracer captures phase names for assertions.
type recordingTracer struct {
	mu  sync.Mutex
	got []Phase
}

func (r *recordingTracer) StartJob(ctx context.Context, _, _ string, _ time.Time) (context.Context, func()) {
	return ctx, func() {}
}

func (r *recordingTracer) Phase(_ context.Context, phase Phase, _, _ time.Time) {
	r.mu.Lock()
	r.got = append(r.got, phase)
	r.mu.Unlock()
}

func (r *recordingTracer) phases() []Phase {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Phase(nil), r.got...)
}

// endCountingTracer counts how many times the job's root span is ended.
type endCountingTracer struct {
	mu       sync.Mutex
	endCount int
}

func (e *endCountingTracer) StartJob(ctx context.Context, _, _ string, _ time.Time) (context.Context, func()) {
	return ctx, func() {
		e.mu.Lock()
		e.endCount++
		e.mu.Unlock()
	}
}

func (e *endCountingTracer) Phase(context.Context, Phase, time.Time, time.Time) {}

func (e *endCountingTracer) ends() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.endCount
}
