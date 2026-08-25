package docker

import (
	"reflect"
	"testing"
)

// structTagDefault returns the envDefault tag of a named field, so a test can
// assert the shipped default without depending on the environment the test
// happens to run in.
func structTagDefault(t *testing.T, v any, field string) string {
	t.Helper()
	f, ok := reflect.TypeOf(v).Elem().FieldByName(field)
	if !ok {
		t.Fatalf("no field %s", field)
	}
	return f.Tag.Get("envDefault")
}

func TestParsePullPolicy(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    PullPolicy
		wantErr bool
	}{
		{"always", PullAlways, false},
		{"if-not-present", PullIfNotPresent, false},
		{"never", PullNever, false},
		// An unset value must mean the historical behaviour, not the fast one.
		{"", PullAlways, false},
		// Kubernetes spelling is deliberately NOT accepted: silently taking
		// "IfNotPresent" would leave two spellings for one setting.
		{"IfNotPresent", "", true},
		{"Always", "", true},
		{"if_not_present", "", true},
		{"yes", "", true},
	} {
		got, err := ParsePullPolicy(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParsePullPolicy(%q) = %q, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParsePullPolicy(%q): unexpected error %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParsePullPolicy(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestShouldPull is the table that decides whether the registry is contacted.
// It is the whole behavioural contract of this change, so both the cached and
// uncached case is pinned for every policy.
func TestShouldPull(t *testing.T) {
	for _, tc := range []struct {
		policy  PullPolicy
		present bool
		want    bool
	}{
		// always: contacts the registry regardless. This is the default and
		// must stay true in both rows, otherwise the change is not backwards
		// compatible.
		{PullAlways, true, true},
		{PullAlways, false, true},

		// if-not-present: the whole point - skip when already local.
		{PullIfNotPresent, true, false},
		{PullIfNotPresent, false, true},

		// never: never contacts the registry, even when the image is missing
		// (the step then fails, which is the intended air-gapped behaviour).
		{PullNever, true, false},
		{PullNever, false, false},

		// A zero-valued PullPolicy must behave like always. A Scheduler built
		// by a caller that never set the field must not silently stop pulling.
		{PullPolicy(""), true, true},
		{PullPolicy(""), false, true},
	} {
		if got := tc.policy.shouldPull(tc.present); got != tc.want {
			t.Errorf("PullPolicy(%q).shouldPull(present=%v) = %v, want %v",
				tc.policy, tc.present, got, tc.want)
		}
	}
}

// TestWithImagePullPolicyRejectsInvalid pins that a bad value fails scheduler
// construction rather than being ignored.
func TestWithImagePullPolicyRejectsInvalid(t *testing.T) {
	s := &Scheduler{}
	if err := WithImagePullPolicy("nonsense")(s); err == nil {
		t.Fatal("WithImagePullPolicy(\"nonsense\") returned nil error")
	}
	if err := WithImagePullPolicy("if-not-present")(s); err != nil {
		t.Fatalf("WithImagePullPolicy(\"if-not-present\"): %v", err)
	}
	if s.imagePullPolicy != PullIfNotPresent {
		t.Errorf("imagePullPolicy = %q, want %q", s.imagePullPolicy, PullIfNotPresent)
	}
}

// TestDefaultSchedulerPullsAlways guards the backwards-compatibility promise at
// the level a user actually gets it: the default configuration.
func TestDefaultSchedulerPullsAlways(t *testing.T) {
	opts := Options{}
	if err := func() error {
		s := &Scheduler{Options: opts}
		return WithImagePullPolicy("")(s)
	}(); err != nil {
		t.Fatalf("empty policy rejected: %v", err)
	}

	var env EnvConfig
	// The envDefault tag is the other half of the promise: an operator who has
	// never heard of this setting must keep the old behaviour.
	if got := structTagDefault(t, &env, "ImagePullPolicy"); got != string(PullAlways) {
		t.Errorf("DOCKER_IMAGE_PULL_POLICY envDefault = %q, want %q", got, PullAlways)
	}
}
