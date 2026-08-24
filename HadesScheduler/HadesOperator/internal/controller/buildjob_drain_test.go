package controller

import (
	"testing"
	"time"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func terminated(name string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  name,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}},
	}
}

func running(name string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  name,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}
}

func status(name string, published bool) buildv1.ContainerStatus {
	return buildv1.ContainerStatus{Name: name, LogsPublished: published}
}

// The drain gate must block deletion while any terminated container still has
// unpublished logs, and must release once every terminated container is published.
// Containers that never terminated (a step after a failing step, or the finalizer
// on a failed job) must not block deletion.
func TestAllTerminatedLogsPublished(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		statuses []buildv1.ContainerStatus
		want     bool
	}{
		{
			name: "succeeded job, all terminated logs published -> drained",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				InitContainerStatuses: []corev1.ContainerStatus{terminated("step-1"), terminated("step-2")},
				ContainerStatuses:     []corev1.ContainerStatus{terminated(FinalizerContainerName)},
			}},
			statuses: []buildv1.ContainerStatus{
				status("step-1", true), status("step-2", true), status(FinalizerContainerName, true),
			},
			want: true,
		},
		{
			name: "one terminated step still unpublished -> not drained",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				InitContainerStatuses: []corev1.ContainerStatus{terminated("step-1"), terminated("step-2")},
			}},
			statuses: []buildv1.ContainerStatus{status("step-1", true), status("step-2", false)},
			want:     false,
		},
		{
			name: "failed job: later step never ran, published steps -> drained (unran does not block)",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				// step-1 terminated (failed), step-2 never ran, finalizer never ran.
				InitContainerStatuses: []corev1.ContainerStatus{terminated("step-1")},
			}},
			statuses: []buildv1.ContainerStatus{
				status("step-1", true), status("step-2", false), status(FinalizerContainerName, false),
			},
			want: true,
		},
		{
			name: "running container does not block, terminated one published -> drained",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				InitContainerStatuses: []corev1.ContainerStatus{terminated("step-1"), running("step-2")},
			}},
			statuses: []buildv1.ContainerStatus{status("step-1", true), status("step-2", false)},
			want:     true,
		},
		{
			name: "terminated finalizer unpublished -> not drained",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				InitContainerStatuses: []corev1.ContainerStatus{terminated("step-1")},
				ContainerStatuses:     []corev1.ContainerStatus{terminated(FinalizerContainerName)},
			}},
			statuses: []buildv1.ContainerStatus{status("step-1", true), status(FinalizerContainerName, false)},
			want:     false,
		},
		{
			name: "terminated container missing from status slice -> not drained",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				InitContainerStatuses: []corev1.ContainerStatus{terminated("step-1")},
			}},
			statuses: []buildv1.ContainerStatus{},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := allTerminatedLogsPublished(tt.pod, tt.statuses); got != tt.want {
				t.Errorf("allTerminatedLogsPublished() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobCompletionTime(t *testing.T) {
	now := time.Now()
	completion := metav1.NewTime(now.Add(-5 * time.Minute))
	condTime := metav1.NewTime(now.Add(-3 * time.Minute))

	t.Run("prefers Status.CompletionTime", func(t *testing.T) {
		job := &batchv1.Job{Status: batchv1.JobStatus{
			CompletionTime: &completion,
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: condTime},
			},
		}}
		if got := jobCompletionTime(job); !got.Equal(completion.Time) {
			t.Errorf("jobCompletionTime() = %v, want %v", got, completion.Time)
		}
	})

	t.Run("falls back to true failed condition transition time", func(t *testing.T) {
		job := &batchv1.Job{Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: condTime},
			},
		}}
		if got := jobCompletionTime(job); !got.Equal(condTime.Time) {
			t.Errorf("jobCompletionTime() = %v, want %v", got, condTime.Time)
		}
	})

	t.Run("ignores non-true conditions", func(t *testing.T) {
		job := &batchv1.Job{Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionFalse, LastTransitionTime: condTime},
			},
		}}
		if got := jobCompletionTime(job); got.Before(now) {
			t.Errorf("jobCompletionTime() = %v, want ~now (>= %v)", got, now)
		}
	})

	t.Run("no info falls back to ~now (not already timed out)", func(t *testing.T) {
		job := &batchv1.Job{}
		got := jobCompletionTime(job)
		if time.Since(got) > DefaultLogDrainTimeout {
			t.Errorf("jobCompletionTime() = %v is already older than DefaultLogDrainTimeout", got)
		}
	})
}
