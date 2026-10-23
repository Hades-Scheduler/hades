package docker

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/hades-scheduler/hades/shared/payload"
)

// The step metadata becomes the container environment and routinely carries
// tokens and API keys, so it must never reach the log at Info level.
func TestLogStepKeepsEnvironmentOutOfInfo(t *testing.T) {
	step := payload.Step{
		ID:       2,
		Name:     "Review",
		Image:    "example/reviewer:1",
		Script:   "echo $API_KEY",
		Metadata: map[string]string{"API_KEY": "secret-value-123"},
	}

	for _, tc := range []struct {
		level      slog.Level
		wantSecret bool
	}{
		{slog.LevelInfo, false},
		{slog.LevelDebug, true},
	} {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: tc.level}))

		logStep(logger, "Executing step", step)

		out := buf.String()
		for _, want := range []string{"step_id=2", "step_name=Review", "image=example/reviewer:1"} {
			if !strings.Contains(out, want) {
				t.Errorf("level %v: log %q lacks %q", tc.level, out, want)
			}
		}
		for _, secret := range []string{"secret-value-123", "echo $API_KEY"} {
			if got := strings.Contains(out, secret); got != tc.wantSecret {
				t.Errorf("level %v: contains %q = %v, want %v\n%s", tc.level, secret, got, tc.wantSecret, out)
			}
		}
	}
}
