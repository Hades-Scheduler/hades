package main

import (
	"os"
	"testing"

	hadesnats "github.com/hades-scheduler/hades/shared/nats"
	"github.com/hades-scheduler/hades/shared/utils"
)

// TestConsumerConfigDefaultsAreLoaded pins that the envDefault tags on the
// nested ConsumerConfig actually apply when the environment sets nothing.
//
// This matters because Concurrency lives in a nested struct rather than on
// HadesSchedulerConfig directly. If nested traversal ever stopped working -
// for instance by making the field a pointer, or by switching config loader -
// the fields would silently stay at their zero values. NewHadesConsumer's
// withDefaults() would still run the consumer correctly, so nothing would
// break at runtime; the damage would be confined to the startup log reporting
// 0 / 0s / 0 while the consumer actually used 1 / 1m / 3. A configuration log
// that disagrees with the effective configuration is worse than no log.
func TestConsumerConfigDefaultsAreLoaded(t *testing.T) {
	// Clear rather than set: t.Setenv with an empty string is still a set
	// variable, and an explicit empty value is not the same as an absent one.
	// Restored by t.Cleanup so the test does not leak into its neighbours.
	for _, key := range []string{"CONCURRENCY", "NATS_ACK_WAIT", "NATS_MAX_DELIVER"} {
		if old, ok := os.LookupEnv(key); ok {
			t.Cleanup(func() {
				if err := os.Setenv(key, old); err != nil {
					t.Errorf("restoring %s: %v", key, err)
				}
			})
		}
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unsetting %s: %v", key, err)
		}
	}

	var cfg HadesSchedulerConfig
	if err := utils.LoadConfig(&cfg); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if got := cfg.ConsumerConfig.Concurrency; got != hadesnats.DefaultConcurrency {
		t.Errorf("Concurrency = %d, want %d", got, hadesnats.DefaultConcurrency)
	}
	if got := cfg.ConsumerConfig.AckWait; got != hadesnats.DefaultAckWait {
		t.Errorf("AckWait = %v, want %v", got, hadesnats.DefaultAckWait)
	}
	if got := cfg.ConsumerConfig.MaxDeliver; got != hadesnats.DefaultMaxDeliver {
		t.Errorf("MaxDeliver = %d, want %d", got, hadesnats.DefaultMaxDeliver)
	}
}
