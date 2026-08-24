package payload

import (
	"strconv"
	"time"

	"github.com/google/uuid"
)

const (
	// DefaultPriority is the default priority level for jobs when not specified
	DefaultPriority = 3

	// MaxTimeoutSeconds bounds TimeoutSeconds so that converting it to a
	// time.Duration (seconds * time.Second) cannot overflow int64 nanoseconds,
	// which would otherwise wrap to a negative duration and fire immediately.
	// math.MaxInt64 / 1e9 ≈ 9.2e9 seconds (~292 years).
	MaxTimeoutSeconds = int64(9223372036)
)

// RESTPayload represents the HTTP request payload for creating a new job.
// It extends QueuePayload with priority information.
type RESTPayload struct {
	Priority int `json:"priority"` // Priority level (1=low, 2=medium, 3+=high)
	QueuePayload
}

// QueuePayload represents a job to be processed by the Hades system.
// It contains all information needed to execute a multi-step job.
type QueuePayload struct {
	ID             uuid.UUID         `json:"id"`                                  // Unique job identifier
	Name           string            `json:"name" binding:"required"`             // Human-readable job name
	Timestamp      time.Time         `json:"timestamp"`                           // Job creation timestamp
	Metadata       map[string]string `json:"metadata"`                            // Additional job-level metadata
	Steps          []Step            `json:"steps"`                               // Ordered list of steps to execute
	CallbackURL    string            `json:"callback_url,omitempty" format:"uri"` // Optional per-job destination for forwarding aggregated logs/results. Must be an absolute http/https URL with a host.
	TimeoutSeconds int64             `json:"timeout_seconds,omitempty"`           // Whole-job timeout in seconds; the job is killed and marked failed once exceeded. 0 = no timeout.
	TraceParent    string            `json:"traceparent,omitempty"`               // W3C trace context propagated from the API so scheduler/operator spans nest under the job trace. Not injected into containers.
}

// Step represents a single execution step in a job.
// Each step runs in its own container with the specified image and resources.
type Step struct {
	ID              int               `json:"id"`                    // Step execution order (starts at 1)
	Name            string            `json:"name"`                  // Human-readable step name
	Image           string            `json:"image"`                 // Container image to use (e.g., "alpine:latest")
	Script          string            `json:"script"`                // Shell script to execute in the container
	ContinueOnError bool              `json:"continue_on_error"`     // Whether to continue with the next step if this step fails
	Metadata        map[string]string `json:"metadata"`              // Step-specific environment variables and metadata
	CPULimit        uint              `json:"cpu_limit"`             // CPU limit in whole cores (e.g., 1 = 1 CPU core, 2 = 2 cores)
	MemoryLimit     string            `json:"memory_limit"`          // Memory limit (e.g., "512M", "2G")
	Network         string            `json:"network,omitempty"`     // Docker network mode: "none", "bridge", "host", "default", or a named network. Empty = executor default. Docker executor only; accepted but not enforced on Kubernetes.
	MemorySwap      string            `json:"memory_swap,omitempty"` // Total memory+swap limit, same format as memory_limit (e.g., "512M", "2G"); requires memory_limit to be set and must be >= memory_limit. Docker executor only.
	PidsLimit       int64             `json:"pids_limit,omitempty"`  // Maximum number of PIDs in the container; 0 = unlimited. Docker executor only.
}

// IDString returns the step ID as a string.
// This is used for generating unique names in Kubernetes resources.
func (s Step) IDString() string {
	return strconv.Itoa(s.ID)
}
