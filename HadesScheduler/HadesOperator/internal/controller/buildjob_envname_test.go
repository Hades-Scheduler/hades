package controller

import (
	"testing"

	hades "github.com/hades-scheduler/hades/shared"
)

// TestEnvFromMetaSkipsInvalidKubernetesNames pins the regression that made the
// Kubernetes executor unable to run ANY job carrying a priority.
//
// The NATS consumer injects hades.tum.de/priority and
// hades.tum.de/priorityName into every job's metadata. The operator turns
// metadata into container env vars, and "/" is not legal in a Kubernetes env
// var name, so the API server rejected the entire Job with
// "Invalid value: \"hades.tum.de/priority\"". The Docker executor accepted the
// same job, so this only ever broke one variant.
func TestEnvFromMetaSkipsInvalidKubernetesNames(t *testing.T) {
	envs := envFromMeta(map[string]string{
		"VALID_NAME":                  "keep",
		"also.valid-name":             "keep",
		hades.MetadataKeyPriority:     "3",
		hades.MetadataKeyPriorityName: "high",
		"has spaces":                  "drop",
		"1leading_digit":              "drop",
	})

	got := map[string]string{}
	for _, e := range envs {
		got[e.Name] = e.Value
	}

	for _, name := range []string{"VALID_NAME", "also.valid-name"} {
		if _, ok := got[name]; !ok {
			t.Errorf("valid env name %q was dropped", name)
		}
	}
	for _, name := range []string{
		hades.MetadataKeyPriority,
		hades.MetadataKeyPriorityName,
		"has spaces",
		"1leading_digit",
	} {
		if _, ok := got[name]; ok {
			t.Errorf("invalid env name %q was passed to Kubernetes; the API server rejects the whole Job", name)
		}
	}
}
