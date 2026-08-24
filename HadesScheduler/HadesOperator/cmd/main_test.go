package main

import (
	"os"
	"testing"
	"time"

	"github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/internal/controller"
	"github.com/hades-scheduler/hades/shared/utils"
)

// An operator started without any tuning env vars must behave exactly as before
// these knobs existed.
func TestOperatorConfigDefaults(t *testing.T) {
	clearOperatorEnv(t)

	cfg := loadOperatorConfig(t)
	cfg.normalize()

	if cfg.RequeueDelay != controller.DefaultRequeueDelay {
		t.Errorf("RequeueDelay = %v, want %v", cfg.RequeueDelay, controller.DefaultRequeueDelay)
	}
	if cfg.LogDrainTimeout != controller.DefaultLogDrainTimeout {
		t.Errorf("LogDrainTimeout = %v, want %v", cfg.LogDrainTimeout, controller.DefaultLogDrainTimeout)
	}
	if cfg.MaxParallelism != DefaultMaxParallelism {
		t.Errorf("MaxParallelism = %d, want %d", cfg.MaxParallelism, DefaultMaxParallelism)
	}
	if !cfg.DeleteOnComplete {
		t.Error("DeleteOnComplete = false, want true")
	}
}

func TestOperatorConfigParsesDurations(t *testing.T) {
	clearOperatorEnv(t)
	t.Setenv("REQUEUE_DELAY", "250ms")
	t.Setenv("LOG_DRAIN_TIMEOUT", "2m")

	cfg := loadOperatorConfig(t)
	cfg.normalize()

	if cfg.RequeueDelay != 250*time.Millisecond {
		t.Errorf("RequeueDelay = %v, want 250ms", cfg.RequeueDelay)
	}
	if cfg.LogDrainTimeout != 2*time.Minute {
		t.Errorf("LogDrainTimeout = %v, want 2m", cfg.LogDrainTimeout)
	}
}

// Zero and negative durations must fall back to the defaults: a non-positive
// RequeueAfter makes controller-runtime requeue immediately and busy-loop.
func TestOperatorConfigNormalizeFallsBackOnNonPositive(t *testing.T) {
	tests := []struct {
		name            string
		requeueDelay    string
		logDrainTimeout string
	}{
		{"zero", "0s", "0s"},
		{"negative", "-1s", "-30s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearOperatorEnv(t)
			t.Setenv("REQUEUE_DELAY", tt.requeueDelay)
			t.Setenv("LOG_DRAIN_TIMEOUT", tt.logDrainTimeout)
			t.Setenv("MAX_PARALLELISM", "0")

			cfg := loadOperatorConfig(t)
			cfg.normalize()

			if cfg.RequeueDelay != controller.DefaultRequeueDelay {
				t.Errorf("RequeueDelay = %v, want %v", cfg.RequeueDelay, controller.DefaultRequeueDelay)
			}
			if cfg.LogDrainTimeout != controller.DefaultLogDrainTimeout {
				t.Errorf("LogDrainTimeout = %v, want %v", cfg.LogDrainTimeout, controller.DefaultLogDrainTimeout)
			}
			if cfg.MaxParallelism != DefaultMaxParallelism {
				t.Errorf("MaxParallelism = %d, want %d", cfg.MaxParallelism, DefaultMaxParallelism)
			}
		})
	}
}

// An unparseable duration is a startup error, not a silent default: main exits
// so the misconfiguration is visible.
func TestOperatorConfigRejectsUnparseableDuration(t *testing.T) {
	clearOperatorEnv(t)
	t.Setenv("REQUEUE_DELAY", "soon")

	var cfg OperatorConfig
	if err := utils.LoadConfig(&cfg); err == nil {
		t.Fatal("LoadConfig() = nil error, want a parse error for REQUEUE_DELAY=soon")
	}
}

func loadOperatorConfig(t *testing.T) OperatorConfig {
	t.Helper()
	var cfg OperatorConfig
	if err := utils.LoadConfig(&cfg); err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	return cfg
}

// clearOperatorEnv unsets the operator's tuning variables so a developer's shell
// cannot leak into the assertions, restoring them when the test ends.
func clearOperatorEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"DELETE_ON_COMPLETE", "MAX_PARALLELISM", "REQUEUE_DELAY", "LOG_DRAIN_TIMEOUT"} {
		if old, ok := os.LookupEnv(k); ok {
			t.Cleanup(func() { _ = os.Setenv(k, old) })
		}
		if err := os.Unsetenv(k); err != nil {
			t.Fatalf("unset %s: %v", k, err)
		}
	}
}
