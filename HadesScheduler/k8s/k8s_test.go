package k8s

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	status "github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/dynamic"
)

// TestBuildBuildJobObject verifies the payload-to-BuildJob mapping used by the
// operator-mode scheduler: name, namespace, labels and steps.
func TestBuildBuildJobObject(t *testing.T) {
	jobID := uuid.New()
	job := payload.QueuePayload{
		ID:   jobID,
		Name: "test-job",
		Steps: []payload.Step{
			{ID: 1, Name: "build", Image: "golang:1.21", Script: "go build"},
			{ID: 2, Name: "test", Image: "golang:1.21", Script: "go test"},
		},
	}

	obj := buildBuildJobObject(job, "hades-executor")

	assert.Equal(t, "build.hades.tum.de/v1", obj.GetAPIVersion())
	assert.Equal(t, "BuildJob", obj.GetKind())
	assert.Equal(t, jobID.String(), obj.GetName())
	assert.Equal(t, "hades-executor", obj.GetNamespace())
	assert.Equal(t, jobID.String(), obj.GetLabels()["hades/job-id"])
	assert.Equal(t, "scheduler", obj.GetLabels()["hades/source"])

	spec, ok := obj.Object["spec"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "test-job", spec["name"])

	steps, ok := spec["steps"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, steps, 2)
	assert.Equal(t, "build", steps[0]["name"])
	assert.Equal(t, "go test", steps[1]["script"])
}

// TestBuildBuildJobObjectAnnotations verifies the timing/trace annotations are
// set only when there is something to carry, so a job with no submission time or
// trace context does not ship an empty "annotations": {} to the API server.
func TestBuildBuildJobObjectAnnotations(t *testing.T) {
	t.Run("omitted when empty", func(t *testing.T) {
		obj := buildBuildJobObject(payload.QueuePayload{ID: uuid.New(), Name: "j"}, "ns")
		metadata := obj.Object["metadata"].(map[string]interface{})
		_, ok := metadata["annotations"]
		assert.False(t, ok, "annotations key must be absent when there is nothing to set")
	})

	t.Run("set when present", func(t *testing.T) {
		job := payload.QueuePayload{
			ID:          uuid.New(),
			Name:        "j",
			Timestamp:   time.Unix(1000, 0),
			TraceParent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		}
		obj := buildBuildJobObject(job, "ns")
		ann := obj.GetAnnotations()
		assert.NotEmpty(t, ann[AnnotationSubmittedAt])
		assert.Equal(t, job.TraceParent, ann[AnnotationTraceParent])
	})
}

func TestNewK8sSchedulerRejectsNonPositiveRateLimits(t *testing.T) {
	for _, burst := range []string{"0", "-1"} {
		t.Setenv("K8S_CLIENT_BURST", burst)
		_, err := NewK8sScheduler(nil)
		require.ErrorContains(t, err, "K8S_CLIENT_BURST")
	}
	t.Setenv("K8S_CLIENT_BURST", "100")
	for _, qps := range []string{"0", "-1"} {
		t.Setenv("K8S_CLIENT_QPS", qps)
		_, err := NewK8sScheduler(nil)
		require.ErrorContains(t, err, "K8S_CLIENT_QPS")
	}
}

type recordingPublisher struct {
	statuses []status.JobStatus
	reasons  []string
}

func (r *recordingPublisher) PublishJobStatus(_ context.Context, s status.JobStatus, _ string, reason ...string) error {
	r.statuses = append(r.statuses, s)
	r.reasons = append(r.reasons, status.FirstReason(reason...))
	return nil
}

// stubDynamic counts Create calls and returns a canned error. The client-go
// fake cannot be used: it deep-copies the object and panics on the plain Go
// types (int, []map[string]interface{}) the scheduler builds.
type stubDynamic struct {
	dynamic.Interface
	dynamic.NamespaceableResourceInterface
	createErr error
	creates   int
}

func (d *stubDynamic) Resource(schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return d
}

func (d *stubDynamic) Namespace(string) dynamic.ResourceInterface { return d }

func (d *stubDynamic) Create(context.Context, *unstructured.Unstructured, metav1.CreateOptions, ...string) (*unstructured.Unstructured, error) {
	d.creates++
	return nil, d.createErr
}

func newTestScheduler(createErr error) (Scheduler, *recordingPublisher, *stubDynamic) {
	dyn := &stubDynamic{createErr: createErr}
	pub := &recordingPublisher{}
	return Scheduler{dynClient: dyn, namespace: "ns", publisher: pub}, pub, dyn
}

func TestCreateBuildJobCRFailureHandling(t *testing.T) {
	gr := schema.GroupResource{Group: "build.hades.tum.de", Resource: "buildjobs"}
	tests := []struct {
		name       string
		stepID     int
		createErr  error
		wantErr    bool
		wantFailed bool
		wantCreate int
	}{
		{"success", 1, nil, false, false, 1},
		{"invalid from API server", 1, apierrors.NewInvalid(schema.GroupKind{Group: gr.Group, Kind: "BuildJob"}, "x", field.ErrorList{field.Invalid(field.NewPath("spec", "steps"), "[]", "bad")}), true, true, 1},
		{"bad request", 1, apierrors.NewBadRequest("nope"), true, true, 1},
		{"transient conflict", 1, apierrors.NewConflict(gr, "x", errors.New("boom")), true, false, 1},
		{"already exists is idempotent", 1, apierrors.NewAlreadyExists(gr, "x"), false, false, 1},
		{"step id too large", math.MaxInt32 + 1, nil, true, true, 0},
		{"step id zero", 0, nil, true, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, pub, dyn := newTestScheduler(tt.createErr)
			job := payload.QueuePayload{ID: uuid.New(), Steps: []payload.Step{{ID: tt.stepID, Image: "alpine"}}}
			err := s.createBuildJobCR(context.Background(), job)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantCreate, dyn.creates)
			if tt.wantFailed {
				require.Equal(t, []status.JobStatus{status.StatusFailed}, pub.statuses)
				assert.True(t, strings.HasPrefix(pub.reasons[0], "invalid job: "))
			} else {
				assert.Empty(t, pub.statuses)
			}
		})
	}
}

func TestFailInvalidTruncatesReason(t *testing.T) {
	s, pub, _ := newTestScheduler(nil)
	err := s.failInvalid(context.Background(), payload.QueuePayload{ID: uuid.New()}, strings.Repeat("e", 1000))
	require.Error(t, err)
	require.Len(t, pub.reasons, 1)
	assert.LessOrEqual(t, len(pub.reasons[0]), maxReasonLen)
}
