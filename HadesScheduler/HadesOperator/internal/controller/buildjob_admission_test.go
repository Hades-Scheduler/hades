package controller

import (
	"context"
	"fmt"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// The admission limit is decided by counting running Jobs. If that count comes
// from the manager's informer cache it is a read-modify-write race: the cache
// has not yet observed the Jobs created by the preceding reconciles, so each
// one reads the same stale total and the limit stops binding. Measured against
// a real 100-job burst with MaxParallelism=16, 59 Jobs ran at once.
//
// These tests model that by giving the cached client and the APIReader
// deliberately different contents. A reconciler that consults the cache fails
// them; one that consults the API server passes.

func admissionScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	return s
}

func runningJob(name string) *batchv1.Job {
	suspend := false
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "hades",
			Labels:    map[string]string{LabelManagedBy: ManagedByValue},
		},
		Spec: batchv1.JobSpec{Suspend: &suspend},
	}
}

func TestCountActiveJobs_ReadsThroughAPIReaderNotCache(t *testing.T) {
	s := admissionScheme(t)

	// The API server already holds 16 running Jobs. The cache has seen none of
	// them yet - exactly the state a burst produces.
	var live []runtimeObject
	for i := 0; i < 16; i++ {
		live = append(live, runningJob(fmt.Sprintf("job-%02d", i)))
	}
	apiReader := fake.NewClientBuilder().WithScheme(s).WithObjects(toObjects(live)...).Build()
	staleCache := fake.NewClientBuilder().WithScheme(s).Build()

	r := &BuildJobReconciler{Client: staleCache, APIReader: apiReader, MaxParallelism: 16}

	got, err := r.countActiveJobs(context.Background(), "hades")
	if err != nil {
		t.Fatalf("countActiveJobs: %v", err)
	}
	if got != 16 {
		t.Fatalf("countActiveJobs = %d, want 16; a stale cache read returns 0 and the limit stops binding", got)
	}
	if got < r.MaxParallelism {
		t.Fatalf("at %d active against a limit of %d the next Job would be created unsuspended", got, r.MaxParallelism)
	}
}

func TestCountActiveJobs_FallsBackToClientWhenAPIReaderUnset(t *testing.T) {
	s := admissionScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(runningJob("job-0")).Build()

	// APIReader deliberately nil: callers that have not wired it must still work.
	r := &BuildJobReconciler{Client: c, MaxParallelism: 16}

	got, err := r.countActiveJobs(context.Background(), "hades")
	if err != nil {
		t.Fatalf("countActiveJobs: %v", err)
	}
	if got != 1 {
		t.Fatalf("countActiveJobs = %d, want 1", got)
	}
}

func TestCountActiveJobs_IgnoresSuspendedJobs(t *testing.T) {
	s := admissionScheme(t)
	suspended := runningJob("suspended-0")
	yes := true
	suspended.Spec.Suspend = &yes

	apiReader := fake.NewClientBuilder().WithScheme(s).
		WithObjects(runningJob("running-0"), suspended).Build()
	r := &BuildJobReconciler{Client: fake.NewClientBuilder().WithScheme(s).Build(), APIReader: apiReader}

	got, err := r.countActiveJobs(context.Background(), "hades")
	if err != nil {
		t.Fatalf("countActiveJobs: %v", err)
	}
	if got != 1 {
		t.Fatalf("countActiveJobs = %d, want 1; a suspended Job holds no capacity", got)
	}
}

type runtimeObject = client.Object

func toObjects(in []runtimeObject) []client.Object { return in }

func TestAdmitOneSuspendedJob_DoesNotExceedTheLimit(t *testing.T) {
	s := admissionScheme(t)
	yes := true

	// Already at the limit, with work waiting. Admitting anything here
	// overshoots; several completion paths call this for the same batch of
	// finishing Jobs, so "one out, one in" is not a safe assumption.
	objs := []client.Object{}
	for i := 0; i < 16; i++ {
		objs = append(objs, runningJob(fmt.Sprintf("running-%02d", i)))
	}
	waiting := runningJob("waiting-0")
	waiting.Spec.Suspend = &yes
	objs = append(objs, waiting)

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	r := &BuildJobReconciler{Client: c, APIReader: c, MaxParallelism: 16}

	if err := r.admitOneSuspendedJob(context.Background(), "hades"); err != nil {
		t.Fatalf("admitOneSuspendedJob: %v", err)
	}

	var got batchv1.Job
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: "hades", Name: "waiting-0"}, &got); err != nil {
		t.Fatalf("get waiting job: %v", err)
	}
	if got.Spec.Suspend == nil || !*got.Spec.Suspend {
		t.Fatal("waiting-0 was admitted while already at the parallelism limit")
	}
}

func TestAdmitOneSuspendedJob_AdmitsWhenThereIsRoom(t *testing.T) {
	s := admissionScheme(t)
	yes := true
	waiting := runningJob("waiting-0")
	waiting.Spec.Suspend = &yes

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(runningJob("running-0"), waiting).Build()
	r := &BuildJobReconciler{Client: c, APIReader: c, MaxParallelism: 16}

	if err := r.admitOneSuspendedJob(context.Background(), "hades"); err != nil {
		t.Fatalf("admitOneSuspendedJob: %v", err)
	}

	var got batchv1.Job
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: "hades", Name: "waiting-0"}, &got); err != nil {
		t.Fatalf("get waiting job: %v", err)
	}
	if got.Spec.Suspend != nil && *got.Spec.Suspend {
		t.Fatal("waiting-0 was not admitted although the limit allowed it")
	}
}
