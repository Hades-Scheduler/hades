package docker

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/hades-scheduler/hades/shared/payload"
)

// The step metadata becomes the container environment and routinely carries
// tokens and API keys, so the raw values must reach the log at no level.
func TestLogStepKeepsEnvironmentOutOfInfo(t *testing.T) {
	step := payload.Step{
		ID:       2,
		Name:     "Review",
		Image:    "example/reviewer:1",
		Script:   "echo $API_KEY",
		Metadata: map[string]string{"API_KEY": "secret-value-123"},
	}

	for _, tc := range []struct {
		level slog.Level
	}{
		{slog.LevelInfo},
		{slog.LevelDebug},
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
		if strings.Contains(out, "secret-value-123") {
			t.Errorf("level %v: log leaks the metadata value\n%s", tc.level, out)
		}
	}
}
