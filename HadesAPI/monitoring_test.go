package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/hades-scheduler/hades/shared/payload"
)

// baseJob returns a QueuePayload with sensitive metadata on the job and its steps.
func baseJob() payload.QueuePayload {
	return payload.QueuePayload{
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		Name:     "test-job",
		Metadata: map[string]string{"secret": "value", "token": "abc123"},
		Steps: []payload.Step{
			{ID: 1, Name: "build", Image: "alpine", Metadata: map[string]string{"key": "val"}},
			{ID: 2, Name: "test", Image: "golang", Metadata: map[string]string{"pass": "word"}},
		},
	}
}

// assertSanitized verifies that the result is valid JSON with empty metadata.
func assertSanitized(t *testing.T, result string) {
	t.Helper()

	if result == "" {
		t.Fatal("expected non-empty result, got empty string")
	}

	var out payload.QueuePayload
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}

	if len(out.Metadata) != 0 {
		t.Errorf("expected job metadata to be empty, got %v", out.Metadata)
	}
	for i, step := range out.Steps {
		if len(step.Metadata) != 0 {
			t.Errorf("expected step[%d] metadata to be empty, got %v", i, step.Metadata)
		}
	}
}

// assertPreserved verifies that non-metadata fields survive sanitization.
func assertPreserved(t *testing.T, result string, original payload.QueuePayload) {
	t.Helper()

	var out payload.QueuePayload
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}

	if out.ID != original.ID {
		t.Errorf("ID changed: got %v, want %v", out.ID, original.ID)
	}
	if out.Name != original.Name {
		t.Errorf("Name changed: got %q, want %q", out.Name, original.Name)
	}
	if len(out.Steps) != len(original.Steps) {
		t.Errorf("Steps length changed: got %d, want %d", len(out.Steps), len(original.Steps))
		return
	}
	for i, step := range out.Steps {
		if step.Name != original.Steps[i].Name {
			t.Errorf("step[%d].Name changed: got %q, want %q", i, step.Name, original.Steps[i].Name)
		}
		if step.Image != original.Steps[i].Image {
			t.Errorf("step[%d].Image changed: got %q, want %q", i, step.Image, original.Steps[i].Image)
		}
	}
}

func TestSafePayloadFormat_MetadataIsStripped(t *testing.T) {
	job := baseJob()
	result := SafePayloadFormat(job)
	assertSanitized(t, result)
}

func TestSafePayloadFormat_NonMetadataFieldsPreserved(t *testing.T) {
	job := baseJob()
	result := SafePayloadFormat(job)
	assertPreserved(t, result, job)
}

func TestSafePayloadFormat_AllInputTypesAreEquivalent(t *testing.T) {
	job := baseJob()
	jobBytes, _ := json.Marshal(job)
	jobString := string(jobBytes)

	fromStruct := SafePayloadFormat(job)
	fromBytes := SafePayloadFormat(jobBytes)
	fromString := SafePayloadFormat(jobString)

	if fromStruct != fromBytes {
		t.Errorf("QueuePayload and []byte results differ:\n  struct: %s\n  bytes:  %s", fromStruct, fromBytes)
	}
	if fromStruct != fromString {
		t.Errorf("QueuePayload and string results differ:\n  struct: %s\n  string: %s", fromStruct, fromString)
	}
}

func TestSafePayloadFormat_InvalidJSONReturnsEmpty(t *testing.T) {
	if result := SafePayloadFormat([]byte("invalid json")); result != "" {
		t.Errorf("expected empty string for invalid []byte, got %q", result)
	}
	if result := SafePayloadFormat("invalid json"); result != "" {
		t.Errorf("expected empty string for invalid string, got %q", result)
	}
}

func TestSafePayloadFormat_EmptyPayload(t *testing.T) {
	result := SafePayloadFormat(payload.QueuePayload{})
	assertSanitized(t, result)
}

func TestSafePayloadFormat_CallbackURLCredentialsStripped(t *testing.T) {
	job := baseJob()
	job.CallbackURL = "https://user:token@example.com/logs?secret=abc#frag"

	result := SafePayloadFormat(job)

	var out payload.QueuePayload
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if out.CallbackURL != "https://example.com/logs" {
		t.Errorf("callback URL not fully redacted: got %q", out.CallbackURL)
	}
	for _, leak := range []string{"user", "token", "secret", "abc", "frag"} {
		if strings.Contains(out.CallbackURL, leak) {
			t.Errorf("sanitized callback URL leaked %q: %s", leak, out.CallbackURL)
		}
	}
}

func TestSafePayloadFormat_StatusCallbackURLCredentialsStripped(t *testing.T) {
	job := baseJob()
	job.StatusCallbackURL = "https://user:token@example.com/status?secret=abc#frag"

	result := SafePayloadFormat(job)

	var out payload.QueuePayload
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if out.StatusCallbackURL != "https://example.com/status" {
		t.Errorf("status callback URL not fully redacted: got %q", out.StatusCallbackURL)
	}
	for _, leak := range []string{"user", "token", "secret", "abc", "frag"} {
		if strings.Contains(out.StatusCallbackURL, leak) {
			t.Errorf("sanitized status callback URL leaked %q: %s", leak, out.StatusCallbackURL)
		}
	}
}

func TestSafePayloadFormat_UnparseableCallbackURLIsDropped(t *testing.T) {
	job := baseJob()
	// A control character makes url.Parse fail. A value that cannot be parsed
	// cannot be sanitized either, so it must be dropped rather than logged.
	job.CallbackURL = "https://user:token@example.com/\x7flogs?secret=abc"
	job.StatusCallbackURL = "https://user:token@example.com/\x7fstatus?secret=abc"

	result := SafePayloadFormat(job)

	var out payload.QueuePayload
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if out.CallbackURL != "" {
		t.Errorf("unparseable callback URL was not dropped: got %q", out.CallbackURL)
	}
	if out.StatusCallbackURL != "" {
		t.Errorf("unparseable status callback URL was not dropped: got %q", out.StatusCallbackURL)
	}
}

func TestSafePayloadFormat_OriginalNotMutated(t *testing.T) {
	job := baseJob()
	_ = SafePayloadFormat(job)

	if len(job.Metadata) == 0 {
		t.Error("SafePayloadFormat mutated the original job's Metadata")
	}
	for i, step := range job.Steps {
		if len(step.Metadata) == 0 {
			t.Errorf("SafePayloadFormat mutated step[%d].Metadata", i)
		}
	}
}
