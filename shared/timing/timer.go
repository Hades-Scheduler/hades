package timing

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// JobTimer records the phases of a single job to the three sinks (slog,
// Prometheus, tracing) and accumulates the per-job overhead/runtime rollup. It
// is safe for concurrent use so a job whose steps stream logs on goroutines can
// record from more than one goroutine.
type JobTimer struct {
	executor string
	jobID    string
	logger   *slog.Logger

	spanCtx context.Context
	endSpan func()

	mu          sync.Mutex
	overhead    time.Duration
	runtime     time.Duration
	summaryOnce sync.Once
}

// NewJobTimer starts a job timer (and its root span) for jobID running on
// executor ("docker" or "k8s"). ctx is the job's context, used as the parent
// for phase spans; pass a context carrying an extracted upstream trace so the
// job's spans nest under the API root. The returned timer's Summary must be
// called (typically deferred) to end the root span and emit the rollup.
// The optional start sets the root span's begin time; the operator passes the
// job's submission time (its phases are reconstructed after the fact and
// backdated, so a now() root would start after its own children). Omitted or
// zero means "now".
func NewJobTimer(ctx context.Context, executor, jobID string, start ...time.Time) *JobTimer {
	begin := time.Time{}
	if len(start) > 0 {
		begin = start[0]
	}
	spanCtx, end := tracer().StartJob(ctx, executor, jobID, begin)
	return &JobTimer{
		executor: executor,
		jobID:    jobID,
		logger:   slog.Default().With(slog.String("executor", executor), slog.String("job_id", jobID)),
		spanCtx:  spanCtx,
		endSpan:  end,
	}
}

// Context returns the timer's span context so callers can propagate the job
// trace further (e.g. into a Kubernetes BuildJob annotation).
func (t *JobTimer) Context() context.Context {
	if t == nil {
		return context.Background()
	}
	return t.spanCtx
}

// Record logs, observes, spans, and accumulates a single phase measured between
// start and end. step is the 1-based step ID, or 0 for job-level phases. A
// negative duration (clock skew across hosts) is clamped to zero and warned. A
// nil timer is a noop, so callers need not guard.
func (t *JobTimer) Record(step int, phase Phase, start, end time.Time) {
	if t == nil {
		return
	}
	d := end.Sub(start)
	if d < 0 {
		t.logger.Warn("negative phase duration clamped to zero",
			slog.String("phase", string(phase)), slog.Int("step", step), slog.Duration("raw", d))
		d = 0
		end = start
	}

	kind := phase.Kind()
	t.logger.Debug("phase",
		slog.String("phase", string(phase)),
		slog.String("kind", string(kind)),
		slog.Int("step", step),
		slog.Int64("dur_ms", d.Milliseconds()),
	)

	phaseSeconds.WithLabelValues(t.executor, string(phase), string(kind)).Observe(d.Seconds())
	// queue_wait spans the gap before this service began handling the job, i.e. it
	// starts before the root span; emitting it as a child span would invert the
	// trace waterfall. It is still logged, metered, and rolled up - the gap is
	// visible in the trace between the API's enqueue span and the first phase span.
	if phase != PhaseQueueWait {
		tracer().Phase(t.spanCtx, phase, start, end)
	}

	t.mu.Lock()
	if kind == KindRuntime {
		t.runtime += d
	} else {
		t.overhead += d
	}
	t.mu.Unlock()
}

// Time runs fn, records the elapsed time as phase, and returns fn's error. The
// phase is recorded even when fn fails, so a step that errors mid-way still
// emits the phases that ran.
func (t *JobTimer) Time(step int, phase Phase, fn func() error) error {
	start := time.Now()
	err := fn()
	t.Record(step, phase, start, time.Now())
	return err
}

// RecordImagePull records an image_pull phase and additionally splits it by
// whether the image was already present locally (a cache hit), which dominates
// the duration and would otherwise blur cold vs warm pulls. A nil timer is a
// noop.
func (t *JobTimer) RecordImagePull(step int, start, end time.Time, cached bool) {
	if t == nil {
		return
	}
	t.Record(step, PhaseImagePull, start, end)
	d := end.Sub(start)
	if d < 0 {
		d = 0
	}
	imagePullSeconds.WithLabelValues(t.executor, boolLabel(cached)).Observe(d.Seconds())
}

// Summary emits the per-job rollup (one Info-level line), observes the rollup
// histograms, and ends the root span. It is idempotent - guarded by sync.Once -
// so an explicit call plus a deferred one do not double-count. overhead_pct is
// the share of measured wall-clock spent in Hades/Kubernetes overhead rather
// than the user's container.
func (t *JobTimer) Summary() {
	if t == nil {
		return
	}
	t.summaryOnce.Do(func() {
		t.mu.Lock()
		overhead, runtime := t.overhead, t.runtime
		t.mu.Unlock()

		wall := overhead + runtime
		pct := 0.0
		if wall > 0 {
			pct = 100 * float64(overhead) / float64(wall)
		}

		t.logger.Info("job timing summary",
			slog.Int64("overhead_ms", overhead.Milliseconds()),
			slog.Int64("runtime_ms", runtime.Milliseconds()),
			slog.Int64("wall_ms", wall.Milliseconds()),
			slog.Float64("overhead_pct", round1(pct)),
		)

		jobOverheadSeconds.WithLabelValues(t.executor).Observe(overhead.Seconds())
		jobRuntimeSeconds.WithLabelValues(t.executor).Observe(runtime.Seconds())
		jobWallSeconds.WithLabelValues(t.executor).Observe(wall.Seconds())

		t.endSpan()
	})
}

// totals exposes the accumulated overhead/runtime for tests.
func (t *JobTimer) totals() (overhead, runtime time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.overhead, t.runtime
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func round1(f float64) float64 {
	return float64(int64(f*10+0.5)) / 10
}
