package timing

import "github.com/prometheus/client_golang/prometheus"

// Metric names share the "hades" namespace with the counters added in the
// monitoring work, so hades_phase_seconds sits alongside hades_jobs_scheduled_total
// on the same /metrics endpoint.
const metricNamespace = "hades"

// phaseBuckets spans sub-millisecond API calls (e.g. ContainerCreate) through
// multi-minute steps in one exponential range; Prometheus' default buckets
// (5ms-10s) are far too narrow for this spread.
var phaseBuckets = prometheus.ExponentialBucketsRange(0.001, 3600, 24)

// phaseSeconds records the duration of every phase, labelled by executor, phase,
// and kind (overhead/runtime).
var phaseSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: metricNamespace,
	Name:      "phase_seconds",
	Help:      "Duration of each Hades job phase in seconds, labelled by executor, phase, and kind (overhead/runtime).",
	Buckets:   phaseBuckets,
}, []string{"executor", "phase", "kind"})

// imagePullSeconds records image-pull duration split by whether the image was
// already present locally (a cache hit), so cold and warm pulls do not distort
// each other's distribution.
var imagePullSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: metricNamespace,
	Name:      "image_pull_seconds",
	Help:      "Image pull duration in seconds, labelled by executor and whether the image was already cached locally.",
	Buckets:   phaseBuckets,
}, []string{"executor", "cached"})

// jobOverheadSeconds, jobRuntimeSeconds, and jobWallSeconds are the per-job
// rollups: total overhead, total container runtime, and their sum.
var (
	jobOverheadSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricNamespace,
		Name:      "job_overhead_seconds",
		Help:      "Total Hades overhead per job in seconds (sum of all overhead phases).",
		Buckets:   phaseBuckets,
	}, []string{"executor"})

	jobRuntimeSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricNamespace,
		Name:      "job_runtime_seconds",
		Help:      "Total container runtime per job in seconds (sum of all runtime phases).",
		Buckets:   phaseBuckets,
	}, []string{"executor"})

	jobWallSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricNamespace,
		Name:      "job_wall_seconds",
		Help:      "Total measured wall-clock per job in seconds (overhead + runtime).",
		Buckets:   phaseBuckets,
	}, []string{"executor"})
)

// collectors is every collector this package owns, registered together.
func collectors() []prometheus.Collector {
	return []prometheus.Collector{
		phaseSeconds,
		imagePullSeconds,
		jobOverheadSeconds,
		jobRuntimeSeconds,
		jobWallSeconds,
	}
}

// MustRegister registers the timing collectors on reg. Each process calls it
// once with the registry it serves: the default registry for the API and the
// Docker-mode scheduler (exposed by shared/metrics.Serve), and
// controller-runtime's registry for the operator. The collectors are not
// registered on init, so the same package used in two processes never
// double-registers.
func MustRegister(reg prometheus.Registerer) {
	reg.MustRegister(collectors()...)
}
