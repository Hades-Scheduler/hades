package controller

import (
	"context"
	"fmt"
	"testing"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestOrderedContainerStatuses_IsDeterministic(t *testing.T) {
	steps := []buildv1.BuildStep{{ID: 1}, {ID: 2}, {ID: 3}}
	statusMap := map[string]buildv1.ContainerStatus{}
	for _, n := range []string{"zzz-extra", "aaa-extra", FinalizerContainerName, "step-3", "step-1", "step-2"} {
		statusMap[n] = buildv1.ContainerStatus{Name: n}
	}
	want := []string{"step-1", "step-2", "step-3", FinalizerContainerName, "aaa-extra", "zzz-extra"}

	// Map iteration order is randomised per range, so repeat to make a
	// regression to unordered output fail reliably.
	for i := 0; i < 50; i++ {
		got := orderedContainerStatuses(steps, statusMap)
		if len(got) != len(want) {
			t.Fatalf("got %d statuses, want %d", len(got), len(want))
		}
		for j, cs := range got {
			if cs.Name != want[j] {
				t.Fatalf("iteration %d: position %d = %q, want %q", i, j, cs.Name, want[j])
			}
		}
	}
}

func newStatusTestReconciler(t *testing.T, bj *buildv1.BuildJob) *BuildJobReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := buildv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(bj).WithStatusSubresource(&buildv1.BuildJob{}).Build()
	return &BuildJobReconciler{Client: c}
}

func TestWriteContainerStatuses_SkipsUnchanged(t *testing.T) {
	ctx := context.Background()
	bj := &buildv1.BuildJob{ObjectMeta: metav1.ObjectMeta{Name: "job-1", Namespace: "ns"}}
	r := newStatusTestReconciler(t, bj)
	statuses := []buildv1.ContainerStatus{
		{Name: "step-1", StepID: 1, State: buildv1.ContainerStateRunning},
		{Name: FinalizerContainerName, State: buildv1.ContainerStatePending},
	}

	load := func() buildv1.BuildJob {
		var got buildv1.BuildJob
		if err := r.Get(ctx, client.ObjectKeyFromObject(bj), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	first := load()
	if err := r.writeContainerStatuses(ctx, &first, statuses, 1, "pod-1"); err != nil {
		t.Fatal(err)
	}
	afterFirst := load()
	if afterFirst.ResourceVersion == bj.ResourceVersion {
		t.Fatal("first write did not change the resourceVersion")
	}

	// Same observed state: must not touch the object.
	second := load()
	if err := r.writeContainerStatuses(ctx, &second, append([]buildv1.ContainerStatus(nil), statuses...), 1, "pod-1"); err != nil {
		t.Fatal(err)
	}
	if got := load(); got.ResourceVersion != afterFirst.ResourceVersion {
		t.Errorf("resourceVersion %s -> %s, want unchanged for identical status", afterFirst.ResourceVersion, got.ResourceVersion)
	}

	// Each field that differs must still produce a write.
	changed := []struct {
		name     string
		statuses []buildv1.ContainerStatus
		step     int32
		pod      string
	}{
		{"container state", []buildv1.ContainerStatus{{Name: "step-1", StepID: 1, State: buildv1.ContainerStateSucceeded}, statuses[1]}, 1, "pod-1"},
		{"current step", statuses, 2, "pod-1"},
		{"pod name", statuses, 1, "pod-2"},
	}
	for _, c := range changed {
		t.Run(c.name, func(t *testing.T) {
			before := load()
			cur := load()
			if err := r.writeContainerStatuses(ctx, &cur, c.statuses, c.step, c.pod); err != nil {
				t.Fatal(err)
			}
			if after := load(); after.ResourceVersion == before.ResourceVersion {
				t.Error("resourceVersion unchanged, want a write")
			}
			// Restore a known baseline for the next case.
			base := load()
			if err := r.writeContainerStatuses(ctx, &base, statuses, 1, "pod-1"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestParseImagePullPolicy(t *testing.T) {
	tests := []struct {
		in      string
		want    corev1.PullPolicy
		wantErr bool
	}{
		{"", "", false},
		{"always", corev1.PullAlways, false},
		{"Always", corev1.PullAlways, false},
		{" IF-NOT-PRESENT ", corev1.PullIfNotPresent, false},
		{"never", corev1.PullNever, false},
		{"IfNotPresent", "", true},
		{"sometimes", "", true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q", tt.in), func(t *testing.T) {
			got, err := ParseImagePullPolicy(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildK8sJob_AppliesImagePullPolicyToSteps(t *testing.T) {
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec: buildv1.BuildJobSpec{Steps: []buildv1.BuildStep{
			{ID: 1, Image: "alpine:latest"},
			{ID: 2, Image: "alpine:latest"},
		}},
	}

	job := buildK8sJob(bj, "job-1", true, false, DefaultFinalizerImage, corev1.PullIfNotPresent)
	for _, c := range job.Spec.Template.Spec.InitContainers {
		if c.ImagePullPolicy != corev1.PullIfNotPresent {
			t.Errorf("%s ImagePullPolicy = %q, want IfNotPresent", c.Name, c.ImagePullPolicy)
		}
	}

	job = buildK8sJob(bj, "job-1", true, false, DefaultFinalizerImage, "")
	for _, c := range job.Spec.Template.Spec.InitContainers {
		if c.ImagePullPolicy != "" {
			t.Errorf("%s ImagePullPolicy = %q, want unset", c.Name, c.ImagePullPolicy)
		}
	}
	if got := job.Spec.Template.Spec.Containers[0].ImagePullPolicy; got != corev1.PullIfNotPresent {
		t.Errorf("finalizer ImagePullPolicy = %q, want IfNotPresent regardless of step policy", got)
	}
}
