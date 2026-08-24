package timing

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope for Hades timing spans.
const tracerName = "github.com/hades-scheduler/hades/shared/timing"

// propagator is the W3C trace-context propagator used to carry a trace across
// NATS (via the job payload) and into Kubernetes (via a BuildJob annotation).
var propagator = propagation.TraceContext{}

// InitTracing configures OpenTelemetry tracing from the standard OTEL_* env and
// installs the OTLP-backed tracer as this package's tracing sink. When
// OTEL_EXPORTER_OTLP_ENDPOINT is unset it leaves the noop tracer in place and
// returns a no-op shutdown, so tracing costs nothing unless a backend is
// configured. serviceName names the service in traces (overridable by
// OTEL_SERVICE_NAME). The returned shutdown flushes and stops the exporter.
func InitTracing(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	// The signal-specific endpoint takes precedence over the generic one and is
	// honoured on its own, matching the OTel exporter's env resolution; if neither
	// is set, tracing stays a noop.
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if endpoint == "" {
		slog.Debug("OTEL_EXPORTER_OTLP_(TRACES_)ENDPOINT unset; tracing disabled")
		return func(context.Context) error { return nil }, nil
	}

	if v := os.Getenv("OTEL_SERVICE_NAME"); v != "" {
		serviceName = v
	}

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}

	// NewSchemaless avoids a schema-URL conflict with resource.Default() (whose
	// schema tracks the SDK's semconv version); resource.Merge errors on differing
	// non-empty schema URLs, which would otherwise drop the service name and leave
	// spans labelled "unknown_service".
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(serviceName),
	))
	if err != nil {
		res = resource.Default()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagator)
	SetTracer(otelTracer{tracer: tp.Tracer(tracerName)})

	slog.Info("Tracing enabled", "service", serviceName, "endpoint", endpoint)
	return tp.Shutdown, nil
}

// StartSpan starts a span named name as a child of ctx and returns the new
// context plus a function that ends it. It uses the globally configured tracer,
// so it is a noop until InitTracing runs. The API uses it to open the enqueue
// span whose context it propagates into the job payload.
func StartSpan(ctx context.Context, name string) (context.Context, func()) {
	ctx, span := otel.Tracer(tracerName).Start(ctx, name)
	return ctx, func() { span.End() }
}

// Inject serializes the trace context in ctx into a carrier map (e.g. the job
// payload's traceparent field or a BuildJob annotation set). Returns nil when
// there is no active span to propagate.
func Inject(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	return carrier
}

// Extract returns a context carrying the trace context encoded in carrier, so a
// downstream JobTimer's spans nest under the upstream root. A nil/empty carrier
// yields ctx unchanged.
func Extract(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier(carrier))
}

// otelTracer is the OpenTelemetry-backed Tracer installed by InitTracing.
type otelTracer struct {
	tracer trace.Tracer
}

func (o otelTracer) StartJob(ctx context.Context, executor, jobID string, start time.Time) (context.Context, func()) {
	opts := []trace.SpanStartOption{trace.WithAttributes(
		attribute.String("hades.executor", executor),
		attribute.String("hades.job_id", jobID),
	)}
	if !start.IsZero() {
		opts = append(opts, trace.WithTimestamp(start))
	}
	ctx, span := o.tracer.Start(ctx, "hades.job", opts...)
	return ctx, func() { span.End() }
}

func (o otelTracer) Phase(ctx context.Context, phase Phase, start, end time.Time) {
	_, span := o.tracer.Start(ctx, string(phase),
		trace.WithTimestamp(start),
		trace.WithAttributes(attribute.String("hades.kind", string(phase.Kind()))),
	)
	span.End(trace.WithTimestamp(end))
}
