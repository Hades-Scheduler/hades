package controller

import (
	"fmt"
	"strings"
	"testing"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func initContainer(t *testing.T, job *batchv1.Job, name string) corev1.Container {
	t.Helper()
	for _, c := range job.Spec.Template.Spec.InitContainers {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("init container %q not found", name)
	return corev1.Container{}
}

func envValue(c corev1.Container, name string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func envCount(c corev1.Container, name string) int {
	n := 0
	for _, e := range c.Env {
		if e.Name == name {
			n++
		}
	}
	return n
}

// A continueOnError step must not abort the pod: its init container has to exit 0
// even when the script uses `set -e` or an explicit non-zero exit, so later steps
// (e.g. the result parser) still run. A normal step must keep failing on error.
func TestBuildK8sJob_ContinueOnError(t *testing.T) {
	failingScript := "set -e; ./gradlew test"
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec: buildv1.BuildJobSpec{
			Name: "job-1",
			Steps: []buildv1.BuildStep{
				{ID: 1, Name: "clone", Image: "alpine", Script: "echo clone"},
				{ID: 2, Name: "test", Image: "alpine", Script: failingScript, ContinueOnError: true},
			},
		},
	}

	job := buildK8sJob(bj, "job-1", true, false)

	// Normal step: run the script directly so a failure fails the container.
	normal := initContainer(t, job, fmt.Sprintf(BuildStepPrefix, 1))
	if len(normal.Args) != 1 || normal.Args[0] != "echo clone" {
		t.Errorf("normal step args = %v, want [\"echo clone\"]", normal.Args)
	}
	if _, ok := envValue(normal, "HADES_STEP_SCRIPT"); ok {
		t.Errorf("normal step should not set HADES_STEP_SCRIPT")
	}

	// continueOnError step: script runs in a nested shell whose status is ignored.
	coe := initContainer(t, job, fmt.Sprintf(BuildStepPrefix, 2))
	wantArgs := `/bin/sh -c "$HADES_STEP_SCRIPT" || true`
	if len(coe.Args) != 1 || coe.Args[0] != wantArgs {
		t.Errorf("continueOnError step args = %v, want [%q]", coe.Args, wantArgs)
	}
	if got, ok := envValue(coe, "HADES_STEP_SCRIPT"); !ok || got != failingScript {
		t.Errorf("continueOnError step HADES_STEP_SCRIPT = %q (present=%v), want %q", got, ok, failingScript)
	}
}

// Job-level metadata must be injected into every step's environment (matching
// Docker/legacy modes), with step metadata overriding job metadata on key
// collision and the reserved UUID/JOB_NAME keys always winning.
func TestBuildK8sJob_JobMetadataInjected(t *testing.T) {
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec: buildv1.BuildJobSpec{
			Name: "pipeline-name",
			Metadata: map[string]string{
				"MOMOS_RUN_ID": "abc",
				"SHARED":       "job",
				"UUID":         "bogus", // must not override the reserved value
			},
			Steps: []buildv1.BuildStep{
				{
					ID:    1,
					Name:  "run",
					Image: "alpine",
					Metadata: map[string]string{
						"SHARED":    "step", // overrides job-level SHARED
						"STEP_ONLY": "x",
						"JOB_NAME":  "bogus", // must not override the reserved value
					},
				},
			},
		},
	}

	job := buildK8sJob(bj, "job-1", true, false)
	c := initContainer(t, job, fmt.Sprintf(BuildStepPrefix, 1))

	wants := map[string]string{
		"MOMOS_RUN_ID": "abc",           // job metadata reaches the step
		"STEP_ONLY":    "x",             // step metadata present
		"SHARED":       "step",          // step overrides job
		"UUID":         "job-1",         // reserved key wins over metadata
		"JOB_NAME":     "pipeline-name", // reserved key wins over metadata
	}
	for name, want := range wants {
		if got, ok := envValue(c, name); !ok || got != want {
			t.Errorf("env %s = %q (present=%v), want %q", name, got, ok, want)
		}
	}
}

// A continueOnError step passes its script via HADES_STEP_SCRIPT. If metadata
// also sets that key, the container must still carry exactly one entry (the
// reserved script value), never a duplicate env var.
func TestBuildK8sJob_NoDuplicateStepScriptEnv(t *testing.T) {
	script := "set -e; ./gradlew test"
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec: buildv1.BuildJobSpec{
			Name:     "job-1",
			Metadata: map[string]string{"HADES_STEP_SCRIPT": "bogus-job"},
			Steps: []buildv1.BuildStep{
				{
					ID:              1,
					Name:            "test",
					Image:           "alpine",
					Script:          script,
					ContinueOnError: true,
					Metadata:        map[string]string{"HADES_STEP_SCRIPT": "bogus-step"},
				},
			},
		},
	}

	job := buildK8sJob(bj, "job-1", true, false)
	c := initContainer(t, job, fmt.Sprintf(BuildStepPrefix, 1))

	if n := envCount(c, "HADES_STEP_SCRIPT"); n != 1 {
		t.Errorf("HADES_STEP_SCRIPT appears %d times, want exactly 1", n)
	}
	if got, ok := envValue(c, "HADES_STEP_SCRIPT"); !ok || got != script {
		t.Errorf("HADES_STEP_SCRIPT = %q (present=%v), want %q (reserved value wins over metadata)", got, ok, script)
	}
}

// The whole-job timeout must be enforced via the Job's ActiveDeadlineSeconds so
// Kubernetes fails the Job (reason DeadlineExceeded) once it elapses.
func TestBuildK8sJob_ActiveDeadlineSeconds(t *testing.T) {
	timeout := int64(600)
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec: buildv1.BuildJobSpec{
			Name:           "job-1",
			TimeoutSeconds: &timeout,
			Steps: []buildv1.BuildStep{
				{ID: 1, Name: "run", Image: "alpine", Script: "echo hi"},
			},
		},
	}

	job := buildK8sJob(bj, "job-1", true, false)
	if job.Spec.ActiveDeadlineSeconds == nil {
		t.Fatalf("ActiveDeadlineSeconds not set, want %d", timeout)
	}
	if *job.Spec.ActiveDeadlineSeconds != timeout {
		t.Errorf("ActiveDeadlineSeconds = %d, want %d", *job.Spec.ActiveDeadlineSeconds, timeout)
	}
}

// When no timeout is configured, ActiveDeadlineSeconds must stay nil so the Job
// has no deadline (current default behaviour).
func TestBuildK8sJob_NoTimeoutLeavesDeadlineUnset(t *testing.T) {
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec: buildv1.BuildJobSpec{
			Name:  "job-1",
			Steps: []buildv1.BuildStep{{ID: 1, Name: "run", Image: "alpine", Script: "echo hi"}},
		},
	}

	job := buildK8sJob(bj, "job-1", true, false)
	if job.Spec.ActiveDeadlineSeconds != nil {
		t.Errorf("ActiveDeadlineSeconds = %v, want nil", *job.Spec.ActiveDeadlineSeconds)
	}
}

// Every step container must start in the shared volume, matching the Docker
// executor (HadesScheduler/docker/step.go sets WorkingDir: "/shared").
//
// Without this the container inherits whatever WORKDIR its image declares, and
// an identical job document resolves relative paths differently depending on
// which executor runs it. That is how the repository's own benchmark payload
// came to carry INGEST_DIR="./shared/example": correct here, where alpine's
// WORKDIR is "/", and silently wrong under Docker, where it resolved to
// /shared/shared/example and the result parser ingested nothing while still
// exiting 0.
//
// The images are chosen to make the failure visible: an image declaring a
// non-root WORKDIR would previously start somewhere else entirely.
func TestBuildK8sJob_StepsRunInSharedVolume(t *testing.T) {
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec: buildv1.BuildJobSpec{
			Name: "job-1",
			Steps: []buildv1.BuildStep{
				{ID: 1, Name: "scripted", Image: "alpine", Script: "echo hi"},
				// No script: runs the image entrypoint, which is exactly the
				// case the result parser hits.
				{ID: 2, Name: "entrypoint", Image: "ghcr.io/example/parser"},
			},
		},
	}

	job := buildK8sJob(bj, "job-1", true, false)

	for _, id := range []int{1, 2} {
		c := initContainer(t, job, fmt.Sprintf(BuildStepPrefix, id))
		if c.WorkingDir != "/shared" {
			t.Errorf("step %d WorkingDir = %q, want %q", id, c.WorkingDir, "/shared")
		}
		// The working directory is only meaningful if the volume is mounted
		// there, so pin the pair together.
		var mounted bool
		for _, m := range c.VolumeMounts {
			if m.MountPath == "/shared" {
				mounted = true
			}
		}
		if !mounted {
			t.Errorf("step %d has WorkingDir /shared but no volume mounted there", id)
		}
	}
}

// The pod's main container must be pinned and must not re-pull on every job.
//
// An untagged image resolves to ":latest", and Kubernetes forces
// imagePullPolicy: Always for ":latest" regardless of what the node already
// has. That put a registry round trip on the critical path of every build job -
// verified on a running cluster, where the rendered pod spec showed
// `image=busybox pullPolicy=Always` while `busybox:latest` was already present
// in the node's containerd store.
//
// Both halves are asserted: a tag alone would still be re-pulled if someone
// later dropped the explicit policy, and an explicit policy alone would still
// leave the image floating.
func TestBuildK8sJob_FinalizerImageIsPinnedAndNotAlwaysPulled(t *testing.T) {
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec: buildv1.BuildJobSpec{
			Name:  "job-1",
			Steps: []buildv1.BuildStep{{ID: 1, Name: "s", Image: "alpine", Script: "true"}},
		},
	}

	job := buildK8sJob(bj, "job-1", true, false)

	var found bool
	for _, c := range job.Spec.Template.Spec.Containers {
		if c.Name != FinalizerContainerName {
			continue
		}
		found = true

		if !strings.Contains(c.Image, ":") {
			t.Errorf("finalizer image %q has no tag; Kubernetes treats that as "+
				":latest and forces imagePullPolicy: Always", c.Image)
		}
		if strings.HasSuffix(c.Image, ":latest") {
			t.Errorf("finalizer image %q is :latest; Kubernetes forces "+
				"imagePullPolicy: Always for that tag", c.Image)
		}
		if c.ImagePullPolicy == corev1.PullAlways {
			t.Errorf("finalizer imagePullPolicy is Always, so every job pays a " +
				"registry round trip for a container that echoes one line")
		}
		if c.ImagePullPolicy == "" {
			t.Errorf("finalizer imagePullPolicy is unset; it must be explicit so " +
				"the effective policy does not depend on how the tag is spelled")
		}
	}
	if !found {
		t.Fatalf("no %s container in the pod spec", FinalizerContainerName)
	}
}
