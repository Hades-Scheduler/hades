package controller

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	job := buildK8sJob(bj, "job-1", true, false, DefaultFinalizerImage, "", JobDefaults{})

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

	job := buildK8sJob(bj, "job-1", true, false, DefaultFinalizerImage, "", JobDefaults{})
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

	job := buildK8sJob(bj, "job-1", true, false, DefaultFinalizerImage, "", JobDefaults{})
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

	job := buildK8sJob(bj, "job-1", true, false, DefaultFinalizerImage, "", JobDefaults{})
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

	job := buildK8sJob(bj, "job-1", true, false, DefaultFinalizerImage, "", JobDefaults{})
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

	job := buildK8sJob(bj, "job-1", true, false, DefaultFinalizerImage, "", JobDefaults{})

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

	job := buildK8sJob(bj, "job-1", true, false, DefaultFinalizerImage, "", JobDefaults{})

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

// The finalizer image must be overridable, and an unset or blank value must
// fall back to the default rather than producing a pod with an empty image
// (which the API server rejects for every job).
func TestBuildJobReconciler_FinalizerImageFallback(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  string
		want string
	}{
		{"unset", "", DefaultFinalizerImage},
		{"blank", "   ", DefaultFinalizerImage},
		{"override", "registry.internal/busybox:1.36", "registry.internal/busybox:1.36"},
	} {
		r := &BuildJobReconciler{FinalizerImage: tc.set}
		if got := r.finalizerImage(); got != tc.want {
			t.Errorf("%s: finalizerImage() = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// An overridden image must actually reach the rendered pod, not just the
// accessor.
func TestBuildK8sJob_HonoursFinalizerImageOverride(t *testing.T) {
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec: buildv1.BuildJobSpec{
			Name:  "job-1",
			Steps: []buildv1.BuildStep{{ID: 1, Name: "s", Image: "alpine", Script: "true"}},
		},
	}

	const custom = "registry.internal/busybox:1.36"
	job := buildK8sJob(bj, "job-1", true, false, custom, "", JobDefaults{})

	for _, c := range job.Spec.Template.Spec.Containers {
		if c.Name == FinalizerContainerName {
			if c.Image != custom {
				t.Errorf("finalizer image = %q, want %q", c.Image, custom)
			}
			// The pull policy must stay explicit whatever the image is: an
			// override ending in :latest must not silently reintroduce Always.
			if c.ImagePullPolicy != corev1.PullIfNotPresent {
				t.Errorf("finalizer imagePullPolicy = %q, want %q",
					c.ImagePullPolicy, corev1.PullIfNotPresent)
			}
			return
		}
	}
	t.Fatalf("no %s container in the pod spec", FinalizerContainerName)
}

// Job pods run untrusted step code, so they must not mount a service account
// token and every container must forbid privilege escalation.
func TestBuildK8sJob_PodHardening(t *testing.T) {
	bj := &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec: buildv1.BuildJobSpec{
			Steps: []buildv1.BuildStep{{ID: 1, Image: "alpine", Script: "true"}},
		},
	}

	job := buildK8sJob(bj, "job-1", true, false, DefaultFinalizerImage, "", JobDefaults{})
	spec := job.Spec.Template.Spec

	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Error("automountServiceAccountToken must be false")
	}
	if spec.SecurityContext == nil || spec.SecurityContext.SeccompProfile == nil ||
		spec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Error("pod seccomp profile must be RuntimeDefault")
	}
	for _, c := range append(spec.InitContainers, spec.Containers...) {
		if c.SecurityContext == nil || c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation {
			t.Errorf("container %q must set allowPrivilegeEscalation=false", c.Name)
		}
	}
}

func defaultsTestJob(timeout *int64, s buildv1.BuildStep) *buildv1.BuildJob {
	s.ID, s.Image = 1, "alpine"
	return &buildv1.BuildJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job-1"},
		Spec:       buildv1.BuildJobSpec{TimeoutSeconds: timeout, Steps: []buildv1.BuildStep{s}},
	}
}

func TestBuildK8sJob_TimeoutDefaults(t *testing.T) {
	i := func(v int64) *int64 { return &v }
	cases := []struct {
		name     string
		job      *int64
		defaults JobDefaults
		want     *int64
	}{
		{"default applied", nil, JobDefaults{TimeoutSeconds: 3600}, i(3600)},
		{"job timeout wins over default", i(60), JobDefaults{TimeoutSeconds: 3600}, i(60)},
		{"max clamps larger job timeout", i(9000), JobDefaults{TimeoutSeconds: 3600, MaxTimeoutSeconds: 600}, i(600)},
		{"max clamps missing job timeout", nil, JobDefaults{MaxTimeoutSeconds: 600}, i(600)},
		{"smaller job timeout kept under max", i(60), JobDefaults{MaxTimeoutSeconds: 600}, i(60)},
		{"disabled", nil, JobDefaults{}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildK8sJob(defaultsTestJob(tc.job, buildv1.BuildStep{}), "job-1", true, false, DefaultFinalizerImage, "", tc.defaults).Spec.ActiveDeadlineSeconds
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Errorf("ActiveDeadlineSeconds = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildK8sJob_ResourceDefaults(t *testing.T) {
	cpu, mem := resource.MustParse("2"), resource.MustParse("4Gi")
	defaults := JobDefaults{CPULimit: &cpu, MemoryLimit: &mem}

	// Nothing set by the step: both defaults apply and requests follow limits.
	c := buildK8sJob(defaultsTestJob(nil, buildv1.BuildStep{}), "job-1", true, false, DefaultFinalizerImage, "", defaults).Spec.Template.Spec.InitContainers[0]
	if !c.Resources.Limits.Cpu().Equal(cpu) || !c.Resources.Limits.Memory().Equal(mem) {
		t.Errorf("limits = %v, want cpu=2 memory=4Gi", c.Resources.Limits)
	}
	if !c.Resources.Requests.Cpu().Equal(cpu) || !c.Resources.Requests.Memory().Equal(mem) {
		t.Errorf("requests = %v, want to follow limits", c.Resources.Requests)
	}

	// The step sets only memory: its value wins, CPU falls back to the default.
	stepMem := resource.MustParse("512Mi")
	c = buildK8sJob(defaultsTestJob(nil, buildv1.BuildStep{MemoryLimit: &stepMem}), "job-1", true, false, DefaultFinalizerImage, "", defaults).Spec.Template.Spec.InitContainers[0]
	if !c.Resources.Limits.Memory().Equal(stepMem) || !c.Resources.Limits.Cpu().Equal(cpu) {
		t.Errorf("limits = %v, want memory=512Mi cpu=2", c.Resources.Limits)
	}

	// Disabled defaults leave the step unlimited.
	c = buildK8sJob(defaultsTestJob(nil, buildv1.BuildStep{}), "job-1", true, false, DefaultFinalizerImage, "", JobDefaults{}).Spec.Template.Spec.InitContainers[0]
	if len(c.Resources.Limits) != 0 || len(c.Resources.Requests) != 0 {
		t.Errorf("resources = %v, want none", c.Resources)
	}
}

// A request-only step must not get a default limit below its request, and the
// maximum must bound a step even when the defaults are disabled.
func TestBuildK8sJob_RequestAndMaxLimits(t *testing.T) {
	q := resource.MustParse
	cpu, mem := q("2"), q("4Gi")
	maxCPU, maxMem := q("16"), q("32Gi")
	initC := func(step buildv1.BuildStep, d JobDefaults) corev1.Container {
		return buildK8sJob(defaultsTestJob(nil, step), "job-1", true, false, DefaultFinalizerImage, "", d).Spec.Template.Spec.InitContainers[0]
	}

	reqCPU, reqMem := q("8"), q("8Gi")
	c := initC(buildv1.BuildStep{CPURequest: &reqCPU, MemoryRequest: &reqMem}, JobDefaults{CPULimit: &cpu, MemoryLimit: &mem, MaxCPULimit: &maxCPU, MaxMemoryLimit: &maxMem})
	if !c.Resources.Limits.Cpu().Equal(reqCPU) || !c.Resources.Limits.Memory().Equal(reqMem) {
		t.Errorf("limits = %v, want them raised to the requests", c.Resources.Limits)
	}

	// A request above the maximum is clamped, and so is the raised limit.
	bigMem := q("64Gi")
	c = initC(buildv1.BuildStep{MemoryRequest: &bigMem}, JobDefaults{MemoryLimit: &mem, MaxMemoryLimit: &maxMem})
	if !c.Resources.Limits.Memory().Equal(maxMem) || !c.Resources.Requests.Memory().Equal(maxMem) {
		t.Errorf("resources = %v, want limit and request at 32Gi", c.Resources)
	}

	// Only maxima configured: a step without limits gets the maximum as limit,
	// but its (unset) request does not become the maximum.
	c = initC(buildv1.BuildStep{}, JobDefaults{MaxCPULimit: &maxCPU, MaxMemoryLimit: &maxMem})
	if !c.Resources.Limits.Cpu().Equal(maxCPU) || !c.Resources.Limits.Memory().Equal(maxMem) {
		t.Errorf("limits = %v, want the maxima", c.Resources.Limits)
	}
}

func TestBuildK8sJob_SharedVolumeSizeLimit(t *testing.T) {
	size := resource.MustParse("10Gi")
	vol := func(d JobDefaults) *corev1.EmptyDirVolumeSource {
		return buildK8sJob(defaultsTestJob(nil, buildv1.BuildStep{}), "job-1", true, false, DefaultFinalizerImage, "", d).Spec.Template.Spec.Volumes[0].EmptyDir
	}
	if got := vol(JobDefaults{SharedVolumeSizeLimit: &size}).SizeLimit; got == nil || !got.Equal(size) {
		t.Errorf("sizeLimit = %v, want 10Gi", got)
	}
	if got := vol(JobDefaults{}).SizeLimit; got != nil {
		t.Errorf("sizeLimit = %v, want nil when disabled", got)
	}
}

func TestBuildK8sJob_DropsCapabilitiesAndServiceLinks(t *testing.T) {
	spec := buildK8sJob(defaultsTestJob(nil, buildv1.BuildStep{}), "job-1", true, false, DefaultFinalizerImage, "", JobDefaults{}).Spec.Template.Spec
	if spec.EnableServiceLinks == nil || *spec.EnableServiceLinks {
		t.Error("enableServiceLinks must be false")
	}
	for _, c := range append(spec.InitContainers, spec.Containers...) {
		caps := c.SecurityContext.Capabilities
		if caps == nil || !slices.Contains(caps.Drop, "NET_RAW") || !slices.Contains(caps.Drop, "MKNOD") {
			t.Errorf("container %q must drop NET_RAW and MKNOD, got %+v", c.Name, caps)
		}
		if slices.Contains(caps.Drop, "ALL") {
			t.Errorf("container %q must not drop ALL", c.Name)
		}
	}
}

func TestBuildK8sJob_MaxLimitsClamp(t *testing.T) {
	q := resource.MustParse
	maxCPU, maxMem := q("16"), q("32Gi")
	big, bigMem := q("100"), q("1Ti")
	first := func(step buildv1.BuildStep, d JobDefaults) corev1.Container {
		return buildK8sJob(defaultsTestJob(nil, step), "job-1", true, false, DefaultFinalizerImage, "", d).Spec.Template.Spec.InitContainers[0]
	}

	t.Run("step limits above the max are clamped, requests follow", func(t *testing.T) {
		c := first(buildv1.BuildStep{CPULimit: &big, MemoryLimit: &bigMem}, JobDefaults{MaxCPULimit: &maxCPU, MaxMemoryLimit: &maxMem})
		if !c.Resources.Limits.Cpu().Equal(maxCPU) || !c.Resources.Limits.Memory().Equal(maxMem) {
			t.Errorf("limits = %v, want 16 and 32Gi", c.Resources.Limits)
		}
		if !c.Resources.Requests.Cpu().Equal(maxCPU) || !c.Resources.Requests.Memory().Equal(maxMem) {
			t.Errorf("requests = %v, want 16 and 32Gi", c.Resources.Requests)
		}
	})
	t.Run("explicit requests above the max are clamped", func(t *testing.T) {
		c := first(buildv1.BuildStep{CPURequest: &big, MemoryRequest: &bigMem}, JobDefaults{MaxCPULimit: &maxCPU, MaxMemoryLimit: &maxMem})
		if !c.Resources.Requests.Cpu().Equal(maxCPU) || !c.Resources.Requests.Memory().Equal(maxMem) {
			t.Errorf("requests = %v, want 16 and 32Gi", c.Resources.Requests)
		}
	})
	t.Run("values below the max are untouched", func(t *testing.T) {
		small := q("2")
		c := first(buildv1.BuildStep{CPULimit: &small}, JobDefaults{MaxCPULimit: &maxCPU})
		if !c.Resources.Limits.Cpu().Equal(small) {
			t.Errorf("cpu limit = %v, want 2", c.Resources.Limits.Cpu())
		}
	})
	t.Run("defaults above the max are clamped too", func(t *testing.T) {
		c := first(buildv1.BuildStep{}, JobDefaults{CPULimit: &big, MemoryLimit: &bigMem, MaxCPULimit: &maxCPU, MaxMemoryLimit: &maxMem})
		if !c.Resources.Limits.Cpu().Equal(maxCPU) || !c.Resources.Limits.Memory().Equal(maxMem) {
			t.Errorf("limits = %v, want 16 and 32Gi", c.Resources.Limits)
		}
	})
	t.Run("no max leaves the value alone", func(t *testing.T) {
		c := first(buildv1.BuildStep{CPULimit: &big}, JobDefaults{})
		if !c.Resources.Limits.Cpu().Equal(big) {
			t.Errorf("cpu limit = %v, want 100", c.Resources.Limits.Cpu())
		}
	})
}
