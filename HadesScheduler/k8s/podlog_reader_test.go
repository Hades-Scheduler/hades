package k8s

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func jobPod(name, jobName string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: "ns",
		Labels:    map[string]string{"job-name": jobName},
	}}
}

func TestResolvePod(t *testing.T) {
	ctx := context.Background()

	t.Run("returns the single pod of the job without a GET", func(t *testing.T) {
		cs := fake.NewClientset(jobPod("buildjob-abc-x1", "buildjob-abc"), jobPod("buildjob-other-x1", "buildjob-other"))
		pl := PodLogReader{K8sClient: cs, Namespace: "ns", JobID: "abc"}

		pod, err := pl.ResolvePod(ctx)
		require.NoError(t, err)
		assert.Equal(t, "buildjob-abc-x1", pod.Name)
		for _, a := range cs.Actions() {
			assert.NotEqual(t, "get", a.GetVerb(), "ResolvePod should not GET a pod by the job ID")
		}
	})

	t.Run("no pod yet is ErrPodNotFound", func(t *testing.T) {
		pl := PodLogReader{K8sClient: fake.NewClientset(), Namespace: "ns", JobID: "abc"}
		_, err := pl.ResolvePod(ctx)
		assert.ErrorIs(t, err, ErrPodNotFound)
	})

	t.Run("multiple pods is an error", func(t *testing.T) {
		cs := fake.NewClientset(jobPod("buildjob-abc-x1", "buildjob-abc"), jobPod("buildjob-abc-x2", "buildjob-abc"))
		pl := PodLogReader{K8sClient: cs, Namespace: "ns", JobID: "abc"}
		_, err := pl.ResolvePod(ctx)
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrPodNotFound)
	})

	t.Run("list failure is surfaced, not reported as not found", func(t *testing.T) {
		cs := fake.NewClientset()
		cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("boom")
		})
		pl := PodLogReader{K8sClient: cs, Namespace: "ns", JobID: "abc"}
		_, err := pl.ResolvePod(ctx)
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrPodNotFound)
	})
}
