package timing

import (
	"context"
	"sync/atomic"
	"time"
)

// Tracer is the tracing sink behind JobTimer. It is deliberately an interface
// so the metrics/slog core has no hard dependency on OpenTelemetry: until
// InitTracing installs an OTLP-backed tracer, the noop implementation is used
// and span recording costs nothing.
type Tracer interface {
	// StartJob opens the per-job root span and returns a context carrying it
	// plus a function that ends the span. executor and jobID are attached as
	// attributes. start is the span's begin time - the operator passes the job's
	// submission time so its backdated child spans do not precede the root.
	StartJob(ctx context.Context, executor, jobID string, start time.Time) (context.Context, func())
	// Phase records a completed phase as a child span between start and end.
	// Passing the real start/end lets the operator emit backdated spans from
	// Kubernetes pod timestamps.
	Phase(ctx context.Context, phase Phase, start, end time.Time)
}

// defaultTracer is the process-wide tracing sink. InitTracing replaces it; until
// then span recording is a noop. It is an atomic pointer so SetTracer (called
// from main at startup, and from tests) never races with JobTimer reads on other
// goroutines.
var defaultTracer atomic.Pointer[Tracer]

func init() {
	var t Tracer = noopTracer{}
	defaultTracer.Store(&t)
}

// tracer returns the current process-wide tracing sink.
func tracer() Tracer {
	return *defaultTracer.Load()
}

// SetTracer installs t as the process-wide tracing sink. InitTracing calls it;
// tests may call it directly with a recording tracer. A nil t resets to noop.
func SetTracer(t Tracer) {
	if t == nil {
		t = noopTracer{}
	}
	defaultTracer.Store(&t)
}

// noopTracer is the zero-cost default used when tracing is disabled.
type noopTracer struct{}

func (noopTracer) StartJob(ctx context.Context, _, _ string, _ time.Time) (context.Context, func()) {
	return ctx, func() {}
}

func (noopTracer) Phase(context.Context, Phase, time.Time, time.Time) {}
