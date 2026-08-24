package k8s

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
