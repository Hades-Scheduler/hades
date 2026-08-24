// Package timing measures how much overhead Hades adds around a job, broken
// down per step and per phase, for both the Docker and Kubernetes executors.
//
// A JobTimer records each phase to three sinks from the same measurement seam:
// a structured slog event (always on), a Prometheus histogram (scraped via the
// shared/metrics endpoints), and an OpenTelemetry span (a per-job waterfall,
// enabled only when an OTLP endpoint is configured). Every phase is classified
// as overhead (work Hades/Kubernetes does around the container) or runtime (the
// user's container actually executing), so a single per-job summary answers
// "what fraction of wall-clock was Hades overhead".
package timing

// Kind classifies a phase as overhead (Hades/Kubernetes coordination) or
// runtime (the user's container actually executing). It is the basis for the
// per-job overhead/runtime rollup.
type Kind string

const (
	KindOverhead Kind = "overhead"
	KindRuntime  Kind = "runtime"
)

// Phase is a named span in a job's lifecycle. The Docker executor measures
// container phases directly; the Kubernetes operator derives step phases from
// pod container timestamps. Job-level phases (queue_wait/provision/teardown)
// apply to both.
type Phase string

const (
	// Job-level (both executors).
	PhaseQueueWait Phase = "queue_wait" // job submitted -> scheduler starts handling it
	PhaseProvision Phase = "provision"  // setup before the first container runs
	PhaseTeardown  Phase = "teardown"   // cleanup + terminal status reporting

	// Per-step, Docker.
	PhaseImagePull        Phase = "image_pull"
	PhaseContainerCreate  Phase = "container_create"
	PhaseContainerStartup Phase = "container_startup"
	PhaseContainerRun     Phase = "container_run" // runtime
	PhaseLogDrain         Phase = "log_drain"
	PhaseContainerRemove  Phase = "container_remove"

	// Per-step, Kubernetes (derived from pod status).
	PhaseStepWait     Phase = "step_wait"               // scheduling + image pull before the step runs
	PhaseStepRun      Phase = "step_run"                // runtime
	PhaseReconcileLag Phase = "reconcile_detection_lag" // up to requeueDelay of pure poll latency
)

// Kind reports whether the phase is runtime (the container executing) or
// overhead (everything else). Only container_run and step_run are runtime.
func (p Phase) Kind() Kind {
	switch p {
	case PhaseContainerRun, PhaseStepRun:
		return KindRuntime
	default:
		return KindOverhead
	}
}
