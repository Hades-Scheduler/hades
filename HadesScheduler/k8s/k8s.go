// Package k8s implements the Kubernetes executor. It creates a BuildJob custom
// resource for the HadesOperator to reconcile into a batch Job. Access to the
// cluster uses the in-cluster config, falling back to KUBECONFIG when run
// out-of-cluster.
package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hades-scheduler/hades/hadesScheduler/log"
	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/hades-scheduler/hades/shared/utils"
	"github.com/nats-io/nats.go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Scheduler executes jobs on a Kubernetes cluster by creating BuildJob custom
// resources via a dynamic client.
type Scheduler struct {
	dynClient dynamic.Interface
	namespace string
	config    K8sConfig
	publisher log.NATSPublisher
}

// K8sConfig holds the base Kubernetes executor configuration.
type K8sConfig struct {
	// K8sNamespace is the namespace in which the jobs should be scheduled (default: hades-executor)
	K8sNamespace string `env:"K8S_NAMESPACE,notEmpty" envDefault:"hades-executor"`
}

// Annotation keys set on the BuildJob so the operator can reconstruct job
// timing it cannot observe itself: the API submission time (for queue_wait) and
// the W3C trace context (so the operator's spans nest under the job trace).
const (
	AnnotationSubmittedAt = "hades.tum.de/submitted-at"
	AnnotationTraceParent = "hades.tum.de/traceparent"
)

// BuildJobGVRConfig identifies the BuildJob custom resource (group/version/
// resource) the operator-mode scheduler creates. Overridable via env for
// non-default CRD installations.
type BuildJobGVRConfig struct {
	Group    string `env:"BUILDJOB_GROUP,notEmpty"    envDefault:"build.hades.tum.de"`
	Version  string `env:"BUILDJOB_VERSION,notEmpty"  envDefault:"v1"`
	Resource string `env:"BUILDJOB_RESOURCE,notEmpty" envDefault:"buildjobs"`
}

// NewK8sScheduler builds a Scheduler by loading K8sConfig, initializing a
// dynamic client for the cluster, and wiring a NATS log publisher.
func NewK8sScheduler(nc *nats.Conn) (*Scheduler, error) {
	slog.Debug("Initializing Kubernetes scheduler")

	var k8sCfg K8sConfig
	if err := utils.LoadConfig(&k8sCfg); err != nil {
		return nil, fmt.Errorf("loading Kubernetes config: %w", err)
	}
	slog.Debug("Kubernetes config", "namespace", k8sCfg.K8sNamespace)

	slog.Info("Initializing Kubernetes client")
	scheduler, err := initializeOperatorAccess(k8sCfg)
	if err != nil {
		return nil, err
	}

	if nc != nil {
		publisher, err := log.NewNATSPublisher(nc)
		if err != nil {
			return nil, fmt.Errorf("creating NATS publisher: %w", err)
		}
		scheduler.publisher = *publisher
	} else {
		slog.Warn("NATS connection is nil, publisher not created, logs will not be published")
	}

	return &scheduler, nil
}

func initializeOperatorAccess(k8sCfg K8sConfig) (Scheduler, error) {
	slog.Info("Initializing dynamic client for BuildJob custom resources")
	rc, err := rest.InClusterConfig()
	if err != nil {
		slog.Warn("InClusterConfig failed, fallback to KUBECONFIG", "error", err)
		kubeconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: clientcmd.RecommendedHomeFile},
			&clientcmd.ConfigOverrides{},
		)
		rc, err = kubeconfig.ClientConfig()
		if err != nil {
			return Scheduler{}, fmt.Errorf("building rest.Config for operator mode: %w", err)
		}
	}

	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		return Scheduler{}, fmt.Errorf("creating dynamic client: %w", err)
	}

	return Scheduler{
		dynClient: dyn,
		namespace: k8sCfg.K8sNamespace,
		config:    k8sCfg,
	}, nil
}

// ScheduleJob runs a job on the cluster by creating a BuildJob custom resource;
// the operator reconciles it into a batch Job.
func (k Scheduler) ScheduleJob(ctx context.Context, job payload.QueuePayload) error {
	slog.Debug("Scheduling job via Operator (creating BuildJob CR)")
	return k.createBuildJobCR(ctx, job)
}

// ToGVR converts the config into a schema.GroupVersionResource for the dynamic client.
func (c BuildJobGVRConfig) ToGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    c.Group,
		Version:  c.Version,
		Resource: c.Resource,
	}
}

func (k Scheduler) createBuildJobCR(ctx context.Context, job payload.QueuePayload) error {
	if k.dynClient == nil {
		return fmt.Errorf("dynamic client is nil: operator mode not initialized")
	}

	var gvrCfg BuildJobGVRConfig
	if err := utils.LoadConfig(&gvrCfg); err != nil {
		return fmt.Errorf("loading BuildJob GVR config: %w", err)
	}
	buildJobGVR := gvrCfg.ToGVR()

	obj := buildBuildJobObject(job, k.namespace)

	_, err := k.dynClient.Resource(buildJobGVR).Namespace(k.namespace).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			slog.Info("BuildJob already exists (idempotent)", "name", job.ID.String())
			return nil
		}
		return err
	}

	slog.Info("Created BuildJob CR", "name", job.ID.String(), "namespace", k.namespace)
	return nil
}

// buildBuildJobObject maps a queue payload to the unstructured BuildJob custom
// resource the operator reconciles.
func buildBuildJobObject(job payload.QueuePayload, namespace string) *unstructured.Unstructured {
	labels := map[string]interface{}{
		"hades/job-id": job.ID.String(),
		"hades/source": "scheduler",
	}

	if job.Metadata != nil {
		if v, ok := job.Metadata[hades.MetadataKeyPriority]; ok && v != "" {
			labels[hades.MetadataKeyPriority] = v
		}
	}

	// Carry the submission time and trace context to the operator as annotations
	// so it can compute queue_wait and nest its backdated step spans under the
	// job trace. The trace context lives here, not in metadata, so it is never
	// injected into a step container's environment.
	annotations := map[string]interface{}{}
	if !job.Timestamp.IsZero() {
		annotations[AnnotationSubmittedAt] = job.Timestamp.Format(time.RFC3339Nano)
	}
	if job.TraceParent != "" {
		annotations[AnnotationTraceParent] = job.TraceParent
	}

	steps := make([]map[string]interface{}, 0, len(job.Steps))
	for _, s := range job.Steps {
		sm := map[string]interface{}{
			"id":              s.ID,
			"name":            s.Name,
			"image":           s.Image,
			"continueOnError": s.ContinueOnError,
		}
		if s.Script != "" {
			sm["script"] = s.Script
		}
		if len(s.Metadata) > 0 {
			sm["metadata"] = s.Metadata
		}
		if s.CPULimit > 0 {
			sm["cpuLimit"] = fmt.Sprintf("%d", s.CPULimit)
		}
		if s.MemoryLimit != "" {
			sm["memoryLimit"] = s.MemoryLimit
		}
		// network, memorySwap and pidsLimit are forwarded for schema symmetry with
		// the Docker executor, but the operator does not enforce them on the pod
		// (Kubernetes has no per-container field for swap/pids, and all containers
		// in a pod share one network namespace). See buildK8sJob.
		if s.Network != "" {
			sm["network"] = s.Network
		}
		if s.MemorySwap != "" {
			sm["memorySwap"] = s.MemorySwap
		}
		if s.PidsLimit > 0 {
			sm["pidsLimit"] = s.PidsLimit
		}
		steps = append(steps, sm)
	}

	spec := map[string]interface{}{
		"name":     job.Name,
		"metadata": job.Metadata,
		"steps":    steps,
	}
	// Whole-job timeout, enforced by the operator via Job.spec.activeDeadlineSeconds.
	if job.TimeoutSeconds > 0 {
		spec["timeoutSeconds"] = job.TimeoutSeconds
	}

	metadata := map[string]interface{}{
		"name":      job.ID.String(),
		"namespace": namespace,
		"labels":    labels,
	}
	// Only set annotations when there is something to carry, so the CR does not
	// ship an empty "annotations": {} to the API server.
	if len(annotations) > 0 {
		metadata["annotations"] = annotations
	}

	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "build.hades.tum.de/v1",
			"kind":       "BuildJob",
			"metadata":   metadata,
			"spec":       spec,
		},
	}
}
