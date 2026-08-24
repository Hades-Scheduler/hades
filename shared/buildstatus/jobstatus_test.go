package buildstatus

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestJobStatus_String(t *testing.T) {
	tests := []struct {
		name   string
		status JobStatus
		want   string
	}{
		{"Queued", StatusQueued, "Queued"},
		{"Running", StatusRunning, "Running"},
		{"Succeeded", StatusSucceeded, "Succeeded"},
		{"Failed", StatusFailed, "Failed"},
		{"Stopped", StatusStopped, "Stopped"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Errorf("JobStatus.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobStatus_Subject(t *testing.T) {
	tests := []struct {
		name   string
		status JobStatus
		want   string
	}{
		{"Queued", StatusQueued, "hades.jobstatus.Queued"},
		{"Running", StatusRunning, "hades.jobstatus.Running"},
		{"Succeeded", StatusSucceeded, "hades.jobstatus.Succeeded"},
		{"Failed", StatusFailed, "hades.jobstatus.Failed"},
		{"Stopped", StatusStopped, "hades.jobstatus.Stopped"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StatusSubject(tt.status); got != tt.want {
				t.Errorf("StatusSubject() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobStatus_IsValid(t *testing.T) {
	tests := []struct {
		name   string
		status JobStatus
		want   bool
	}{
		{"queued is valid", StatusQueued, true},
		{"running is valid", StatusRunning, true},
		{"succeeded is valid", StatusSucceeded, true},
		{"failed is valid", StatusFailed, true},
		{"stopped is valid", StatusStopped, true},
		{"invalid status", JobStatus("invalid"), false},
		{"empty status", JobStatus(""), false},
		{"random string", JobStatus("random"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.IsValid(); got != tt.want {
				t.Errorf("JobStatus.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFirstReason(t *testing.T) {
	tests := []struct {
		name    string
		reasons []string
		want    string
	}{
		{"none", nil, ""},
		{"single", []string{"ImagePullBackOff"}, "ImagePullBackOff"},
		{"skips empty", []string{"", "boom"}, "boom"},
		{"all empty", []string{"", ""}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FirstReason(tt.reasons...); got != tt.want {
				t.Fatalf("FirstReason(%v) = %q, want %q", tt.reasons, got, tt.want)
			}
		})
	}
}

func TestJobStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		name   string
		status JobStatus
		want   bool
	}{
		{"queued is not terminal", StatusQueued, false},
		{"running is not terminal", StatusRunning, false},
		{"succeeded is terminal", StatusSucceeded, true},
		{"failed is terminal", StatusFailed, true},
		{"stopped is terminal", StatusStopped, true},
		{"unknown is not terminal", JobStatus("random"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.IsTerminal(); got != tt.want {
				t.Errorf("JobStatus.IsTerminal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatusFromSubject(t *testing.T) {
	tests := []struct {
		name    string
		subject string
		want    JobStatus
	}{
		{"succeeded", "hades.jobstatus.Succeeded", StatusSucceeded},
		{"failed", "hades.jobstatus.Failed", StatusFailed},
		{"round trip", StatusSubject(StatusStopped), StatusStopped},
		{"no token", "hades.jobstatus.", JobStatus("")},
		{"no separator", "hadesjobstatus", JobStatus("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := StatusFromSubject(tt.subject); got != tt.want {
				t.Errorf("StatusFromSubject(%q) = %q, want %q", tt.subject, got, tt.want)
			}
		})
	}
}

func TestTruncateReason(t *testing.T) {
	short := "ImagePullBackOff: no such image"
	if got := TruncateReason(short); got != short {
		t.Errorf("TruncateReason(%q) = %q, want it unchanged", short, got)
	}

	if got := TruncateReason(""); got != "" {
		t.Errorf("TruncateReason(\"\") = %q, want \"\"", got)
	}

	exact := strings.Repeat("a", MaxReasonLen)
	if got := TruncateReason(exact); got != exact {
		t.Error("a reason of exactly MaxReasonLen runes must not be truncated")
	}

	// Multi-byte runes: the cap counts runes, not bytes, and must not split one.
	long := strings.Repeat("ü", MaxReasonLen+10)
	got := TruncateReason(long)
	runes := []rune(got)
	// MaxReasonLen is a hard bound: the ellipsis counts towards it.
	if len(runes) != MaxReasonLen {
		t.Errorf("truncated reason has %d runes, want exactly %d including the ellipsis", len(runes), MaxReasonLen)
	}
	if runes[len(runes)-1] != '…' {
		t.Error("a truncated reason must be marked with an ellipsis")
	}
	if !utf8.ValidString(got) {
		t.Error("truncation split a multi-byte rune")
	}
	if again := TruncateReason(got); again != got {
		t.Error("TruncateReason must be idempotent on an already-truncated value")
	}
}
