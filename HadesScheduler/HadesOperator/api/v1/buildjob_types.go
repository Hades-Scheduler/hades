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

// NOTE: Once this file is changed, you must run 'make manifests' to update the CRD manifests.
// Otherwise, the action will fail.

package v1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BuildJobSpec NOTE: BuildJobSpec is intentionally defined separately from the types in shared/payload.
// While it may duplicate some fields from the payload definitions, we cannot directly
// use or combine them due to Kubebuilder requirements:
//  1. CRD types must be defined within this API package (api/v1) for controller-gen.
//  2. This struct requires Kubebuilder markers (e.g., +kubebuilder:validation:...)
//     which would improperly pollute the generic 'shared/payload' package.
//
// **IMPORTANT**: If the API in 'shared/payload/payload.go' changes,
// developers must manually review and update BuildJobSpec here to ensure consistency.

type BuildJobSpec struct {
	// Human-readable name describing this BuildJob's purpose.
	// Example: "Build and Test Assignment", "Deploy Application"
	Name string `json:"name"`

	// Global key-value pairs available to all steps as environment variables.
	// Use this for configuration shared across all steps, such as repository URLs,
	// Example: {"GLOBAL": "test", "ENVIRONMENT": "production"}
	Metadata map[string]string `json:"metadata,omitempty"`

	// Ordered list of execution steps that make up this BuildJob pipeline.
	// Steps run sequentially according to their ID (1, 2, 3, ...).
	// Each step runs in its own container and can share data with other steps via mounted volumes.
	// Typical pipeline: Step 1 clones code, Step 2 builds it, Step 3 runs tests.
	// +kubebuilder:validation:MinItems=1
	Steps []BuildStep `json:"steps"`

	// Maximum time (in seconds) allowed for the entire BuildJob to complete.
	// If this timeout is exceeded, the BuildJob is immediately terminated and marked as Failed.
	// Useful for preventing stuck jobs from consuming resources indefinitely.
	// Example: 3600 = 1 hour, 600 = 10 minutes.
	// +kubebuilder:validation:Minimum=1
	TimeoutSeconds *int64 `json:"timeoutSeconds,omitempty"`

	// Maximum number of times to retry the entire BuildJob if it fails.
	// In CI/CD scenarios, a failure typically indicates the code didn't pass tests,
	// so retrying is usually not beneficial.
	// +kubebuilder:validation:Minimum=0
	MaxRetries *int32 `json:"maxRetries,omitempty"`
}

type BuildStep struct {
	// Unique numeric identifier determining execution order. Must start at 1 and increment by 1.
	// Steps execute in ascending ID order: step with id=1 runs first, then id=2, etc.
	// This is mandatory and must be unique within the BuildJob.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=2147483647
	ID int32 `json:"id"`

	// Descriptive name for this step, shown in logs and UI.
	// Should clearly indicate what the step does.
	// Examples: "Clone Repository", "Run Tests", "Build Docker Image"
	Name string `json:"name,omitempty"`

	// Container image to run for this step. Must be a valid Docker image reference.
	// Can be from any registry (Docker Hub, GHCR, private registry).
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Optional bash script to execute inside the container.
	// If provided, this overrides the container image's default entrypoint/command.
	// The script runs with /bin/sh -c, so you can use shell features like &&, ||, pipes, etc.
	// Common use: chaining commands like "cd /workspace && npm install && npm test"
	// Example: "set -e && cd /shared/example && ./gradlew clean test"
	Script string `json:"script,omitempty"`

	// If true, the BuildJob pipeline continues to the next step even if this step's container
	// exits with a non-zero status. Useful for steps that must always run regardless of prior
	// failures, such as result parsing or cleanup.
	ContinueOnError bool `json:"continueOnError,omitempty"`

	// Step-specific key-value pairs injected as environment variables into this step's container.
	// Use for step-specific configuration like repository URLs, file paths, or credentials.
	// Variables can reference placeholders (e.g., "{{user}}") that get substituted at runtime.
	// Examples:
	// - "REPOSITORY_DIR": "/shared" (where to clone code)
	// - "HADES_TEST_URL": "{{test_repo}}" (test repository URL)
	// - "WORKDIR": "/app/build" (working directory path)
	Metadata map[string]string `json:"metadata,omitempty"`

	// Minimum CPU and memory resources required for this step's container.
	// Follows Kubernetes resource quantity format (e.g., "500m" = 0.5 CPU cores, "2" = 2 cores).
	// Ensures the step has sufficient resources to run without being throttled.
	// If not specified, uses the value of cpuLimit.
	CPURequest *resource.Quantity `json:"cpuRequest,omitempty"`

	// Minimum memory required for this step's container.
	// Follows Kubernetes resource quantity format (e.g., "512Mi", "2Gi", "1G").
	// Ensures the step has minimum memory to avoid out-of-memory errors.
	// If not specified, uses the value of memoryLimit.
	MemoryRequest *resource.Quantity `json:"memoryRequest,omitempty"`

	// Maximum CPU resources this step's container can use.
	// Follows Kubernetes resource quantity format (e.g., "500m" = 0.5 CPU cores, "2" = 2 cores).
	// Prevents a single step from monopolizing cluster resources.
	// If not specified, uses cluster defaults.
	CPULimit *resource.Quantity `json:"cpuLimit,omitempty"`

	// Maximum memory this step's container can use.
	// Follows Kubernetes resource quantity format (e.g., "512Mi", "2Gi", "1G").
	// Prevents out-of-memory issues and resource exhaustion.
	// If not specified, uses cluster defaults.
	MemoryLimit *resource.Quantity `json:"memoryLimit,omitempty"`

	// Docker network mode for this step (e.g. "none", "bridge", "host", or a named network).
	// Docker executor only: accepted for schema parity but NOT enforced on Kubernetes,
	// where all containers in a pod share a single network namespace.
	Network string `json:"network,omitempty"`

	// Total memory+swap limit (e.g. "512M", "2G"), matching the Docker semantics where
	// this is memory + swap combined.
	// Docker executor only: accepted for schema parity but NOT enforced on Kubernetes,
	// which has no per-container swap field.
	MemorySwap string `json:"memorySwap,omitempty"`

	// Maximum number of PIDs allowed in this step's container; 0 means unlimited.
	// Docker executor only: accepted for schema parity but NOT enforced on Kubernetes,
	// which has no per-container PID-limit field.
	PidsLimit int64 `json:"pidsLimit,omitempty"`
}

type BuildJobStatus struct {
	// Current lifecycle phase of the BuildJob:
	// - Pending: Job created but not yet started (waiting for resources or scheduling)
	// - Running: At least one step is currently executing
	// - Succeeded: All steps completed successfully
	// - Failed: At least one step failed, or the job timed out
	// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed
	Phase   string `json:"phase,omitempty"`
	Message string `json:"message,omitempty"`

	// Name of the Kubernetes Pod created by the operator to execute this BuildJob.
	PodName string `json:"podName,omitempty"`

	// Timestamp when the BuildJob starts execution
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// Timestamp when the BuildJob finished execution (either successfully or with failure).
	// Empty if the job is still running or pending.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// ID of the step currently being executed.
	// For example, if currentStep=2, the operator is running the step with id=2.
	// This helps track progress through the pipeline.
	CurrentStep *int32 `json:"currentStep,omitempty"`

	// Number of times this BuildJob has been retried after failures.
	// Increments each time the operator restarts the job due to failure.
	// When retryCount reaches maxRetries, no further attempts are made.
	RetryCount int32 `json:"retryCount,omitempty"`

	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty"`
}

// ContainerState represents the runtime state of a container
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Unknown
type ContainerState string

const (
	ContainerStatePending   ContainerState = "Pending"
	ContainerStateRunning   ContainerState = "Running"
	ContainerStateSucceeded ContainerState = "Succeeded"
	ContainerStateFailed    ContainerState = "Failed"
	ContainerStateUnknown   ContainerState = "Unknown"
)

// ContainerStatus tracks the runtime state of a single container
type ContainerStatus struct {
	// Name of the container (e.g., "step-0", "step-1", "buildjob-finalizer")
	Name string `json:"name"`

	// StepID links back to BuildStep.ID (0 for finalizer container)
	StepID int32 `json:"stepId"`

	// State of the container
	State ContainerState `json:"state"`

	// LogsPublished indicates whether logs have been read and published to NATS
	LogsPublished bool `json:"logsPublished,omitempty"`

	// LogsStreamedUntil records, throttled, the timestamp of the most recent log
	// line streamed to NATS for this container. On an operator restart the live
	// log stream is re-established with this value as the follow start time
	// (PodLogOptions.SinceTime), bounding how many already-published lines are
	// re-delivered. It is intentionally persisted infrequently to avoid a
	// reconcile/etcd write storm.
	LogsStreamedUntil *metav1.Time `json:"logsStreamedUntil,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// BuildJob is the Schema for the buildjobs API in Hades operator mode.
// A BuildJob represents a multi-step CI/CD pipeline execution, where each step runs in a container.
// Steps execute sequentially in order of their ID, with shared data passed between steps via volumes.
type BuildJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BuildJobSpec   `json:"spec,omitempty"`
	Status BuildJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BuildJobList contains a list of BuildJob.
type BuildJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BuildJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BuildJob{}, &BuildJobList{})
}
