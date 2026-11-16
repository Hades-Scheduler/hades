package redact

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/hades-scheduler/hades/shared/payload"
)

func TestValue_KeyDenylist(t *testing.T) {
	r := Default()
	tests := []struct {
		key, val   string
		wantMasked bool
	}{
		{"GIT_PASSWORD", "hunter2", true},
		{"API_TOKEN", "abc", true},
		{"access_key", "x", true},
		{"CI_SECRET", "s", true},
		{"AUTH_HEADER", "Bearer x", true},
		{"REPO_URL", "https://github.com/org/repo", false},
		{"BRANCH", "main", false},
		{"STEP_NAME", "build", false},
		{"empty", "", false},
	}
	for _, tt := range tests {
		got, masked := r.Value(tt.key, tt.val)
		if masked != tt.wantMasked {
			t.Errorf("Value(%q,%q) masked=%v, want %v", tt.key, tt.val, masked, tt.wantMasked)
		}
		if masked && got != Mask {
			t.Errorf("Value(%q,%q)=%q, want mask", tt.key, tt.val, got)
		}
		if !masked && got != tt.val {
			t.Errorf("Value(%q,%q)=%q, want unchanged", tt.key, tt.val, got)
		}
	}
}

func TestValue_ContentHeuristics(t *testing.T) {
	r := Default()
	// Innocuous key names, but sensitive values.
	masked := []struct{ key, val string }{
		{"DATABASE_URL", "postgres://user:s3cr3t@db.host:5432/app"},
		{"CONN", "redis://admin:p@ssw0rd@cache:6379"},
		{"KEY_MATERIAL", "-----BEGIN RSA PRIVATE KEY-----\nMIIE..."},
		{"BLOB", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.dBjftJeZ4CVP"},
		{"RANDOM", "ZmFrZS1oaWdoLWVudHJvcHktdG9rZW4tMTIzNDU2Nzg5MA"},
	}
	for _, tt := range masked {
		if got, m := r.Value(tt.key, tt.val); !m || got != Mask {
			t.Errorf("Value(%q,%q) expected masked, got %q masked=%v", tt.key, tt.val, got, m)
		}
	}
	// Plain values with innocuous keys stay visible.
	visible := []struct{ key, val string }{
		{"REPO_URL", "https://github.com/org/repo.git"},
		{"IMAGE", "alpine:latest"},
		{"COUNT", "10"},
	}
	for _, tt := range visible {
		if got, m := r.Value(tt.key, tt.val); m || got != tt.val {
			t.Errorf("Value(%q,%q) expected visible, got %q masked=%v", tt.key, tt.val, got, m)
		}
	}
}

func TestModeAll_MasksEverything(t *testing.T) {
	r, err := New(Config{Mode: ModeAll})
	if err != nil {
		t.Fatal(err)
	}
	got, masked := r.Value("BRANCH", "main")
	if !masked || got != Mask {
		t.Errorf("ModeAll Value = %q masked=%v, want masked", got, masked)
	}
	// Empty values are still left alone.
	if got, masked := r.Value("X", ""); masked || got != "" {
		t.Errorf("ModeAll on empty = %q masked=%v, want unchanged", got, masked)
	}
}

func TestMetadata_DoesNotMutateInput(t *testing.T) {
	r := Default()
	in := map[string]string{"TOKEN": "secret", "BRANCH": "main"}
	out := r.Metadata(in)
	if in["TOKEN"] != "secret" {
		t.Error("input map was mutated")
	}
	if out["TOKEN"] != Mask {
		t.Errorf("TOKEN not masked: %q", out["TOKEN"])
	}
	if out["BRANCH"] != "main" {
		t.Errorf("BRANCH changed: %q", out["BRANCH"])
	}
}

func TestPayload_RedactsJobAndSteps_NoMutation(t *testing.T) {
	r := Default()
	job := payload.QueuePayload{
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		Name:     "job",
		Metadata: map[string]string{"SECRET": "x", "NAME": "y"},
		Steps: []payload.Step{
			{ID: 1, Name: "a", Image: "alpine", Metadata: map[string]string{"PASSWORD": "p", "BRANCH": "main"}},
		},
	}
	out := r.Payload(job)

	if out.Metadata["SECRET"] != Mask || out.Metadata["NAME"] != "y" {
		t.Errorf("job metadata redaction wrong: %v", out.Metadata)
	}
	if out.Steps[0].Metadata["PASSWORD"] != Mask || out.Steps[0].Metadata["BRANCH"] != "main" {
		t.Errorf("step metadata redaction wrong: %v", out.Steps[0].Metadata)
	}
	// Non-metadata fields preserved.
	if out.Name != "job" || out.Steps[0].Image != "alpine" {
		t.Error("non-metadata fields changed")
	}
	// Original untouched.
	if job.Metadata["SECRET"] != "x" || job.Steps[0].Metadata["PASSWORD"] != "p" {
		t.Error("Payload mutated the original job")
	}
}

func TestDrop_EmptiesMetadata_NoMutation(t *testing.T) {
	job := payload.QueuePayload{
		Metadata: map[string]string{"a": "b"},
		Steps:    []payload.Step{{Metadata: map[string]string{"c": "d"}}},
	}
	out := Drop(job)
	if len(out.Metadata) != 0 || len(out.Steps[0].Metadata) != 0 {
		t.Errorf("Drop left metadata: %v %v", out.Metadata, out.Steps[0].Metadata)
	}
	if len(job.Metadata) == 0 || len(job.Steps[0].Metadata) == 0 {
		t.Error("Drop mutated the original job")
	}
}

func TestText_ScrubsScriptSecrets(t *testing.T) {
	r := Default()
	// The exact leak the reviewer reproduced end-to-end.
	in := "export API_KEY=sk-live-abcdef123456; git clone https://user:p4ssw0rd@github.com/x/y.git"
	out := r.Text(in)
	if strings.Contains(out, "sk-live-abcdef123456") {
		t.Errorf("API_KEY leaked: %q", out)
	}
	if strings.Contains(out, "p4ssw0rd") {
		t.Errorf("URL password leaked: %q", out)
	}
	// Structure preserved.
	if !strings.Contains(out, "git clone https://user:") || !strings.Contains(out, "@github.com/x/y.git") {
		t.Errorf("structure not preserved: %q", out)
	}
}

func TestText_VariousSecretForms(t *testing.T) {
	r := Default()
	cases := []struct{ in, mustNotContain string }{
		{"GIT_PASSWORD=hunter2", "hunter2"},
		{"export AUTH_TOKEN='ghp_abcDEF123456'", "ghp_abcDEF123456"},
		{"curl -H 'Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.dBjftJeZ4CVPmB92K27uhbUJU1p1r'", "dBjftJeZ4CVPmB92K27uhbUJU1p1r"},
		{"echo ZmFrZS1oaWdoLWVudHJvcHktdG9rZW4tMTIzNDU2Nzg5MA > /tmp/x", "ZmFrZS1oaWdoLWVudHJvcHktdG9rZW4tMTIzNDU2Nzg5MA"},
		{"psql postgres://admin:s3cr3t@db:5432/app", "s3cr3t"},
		// a quoted value is masked up to its closing quote
		{"docker login -u ci -p 'alpha beta' registry.example", "beta"},
		{"mytool --password \"alpha beta\" run", "beta"},
		{"mysql -p'alpha beta' -e 'select 1'", "beta"},
	}
	for _, c := range cases {
		out := r.Text(c.in)
		if strings.Contains(out, c.mustNotContain) {
			t.Errorf("Text(%q) leaked %q -> %q", c.in, c.mustNotContain, out)
		}
		if !strings.Contains(out, Mask) {
			t.Errorf("Text(%q) did not mask -> %q", c.in, out)
		}
	}
}

func TestText_LeavesInnocuousScriptsAlone(t *testing.T) {
	r := Default()
	in := "echo 'building the project'\nmake build\nls -la /shared"
	if out := r.Text(in); out != in {
		t.Errorf("innocuous script altered: %q -> %q", in, out)
	}
	if r.Text("") != "" {
		t.Error("empty script should stay empty")
	}
}

func TestPayload_RedactsStepScript(t *testing.T) {
	r := Default()
	job := payload.QueuePayload{
		Steps: []payload.Step{{
			ID:     1,
			Script: "git clone https://u:token123456789012345678@h/r.git",
		}},
	}
	out := r.Payload(job)
	if strings.Contains(out.Steps[0].Script, "token123456789012345678") {
		t.Errorf("step script secret leaked: %q", out.Steps[0].Script)
	}
	// Original not mutated.
	if !strings.Contains(job.Steps[0].Script, "token123456789012345678") {
		t.Error("Payload mutated original step script")
	}
}

func TestNew_InvalidPattern(t *testing.T) {
	if _, err := New(Config{KeyPattern: "([unterminated"}); err == nil {
		t.Error("expected error for invalid regex")
	}
}
