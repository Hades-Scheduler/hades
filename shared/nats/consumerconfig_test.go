package nats

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
)

// TestConsumerConfigEnvDefaultsMatchConstants pins the envDefault tag literals
// to the exported constants they mirror.
//
// Struct tags cannot reference Go constants, so the default values exist twice:
// once as DefaultConcurrency/DefaultAckWait/DefaultMaxDeliver and once as tag
// literals. Nothing but this test stops the two from drifting, and drift would
// be quiet - withDefaults() would keep the consumer running correctly while the
// parsed config, and therefore the scheduler's startup log, reported something
// else.
func TestConsumerConfigEnvDefaultsMatchConstants(t *testing.T) {
	ty := reflect.TypeOf(ConsumerConfig{})
	for _, tc := range []struct {
		field string
		want  string
	}{
		{"Concurrency", "1"},
		{"AckWait", "1m"},
		{"MaxDeliver", "3"},
	} {
		f, ok := ty.FieldByName(tc.field)
		if !ok {
			t.Fatalf("ConsumerConfig has no field %s", tc.field)
		}
		if got := f.Tag.Get("envDefault"); got != tc.want {
			t.Errorf("%s envDefault = %q, want %q", tc.field, got, tc.want)
		}
	}

	// The literals above are only meaningful if they parse to the constants.
	if d, err := time.ParseDuration("1m"); err != nil || d != DefaultAckWait {
		t.Errorf("AckWait tag %q does not equal DefaultAckWait (%v)", "1m", DefaultAckWait)
	}
	if DefaultMaxDeliver != 3 {
		t.Errorf("DefaultMaxDeliver = %d, but the envDefault tag says 3", DefaultMaxDeliver)
	}
	if DefaultConcurrency != 1 {
		t.Errorf("DefaultConcurrency = %d, but the envDefault tag says 1", DefaultConcurrency)
	}
}

// TestConsumerConfigParsesDefaultsFromEmptyEnv is the behavioural half: with none
// of the variables set, a parsed config must already carry the documented
// defaults rather than zeros.
func TestConsumerConfigParsesDefaultsFromEmptyEnv(t *testing.T) {
	// Clear rather than set: t.Setenv with an empty string still leaves the
	// variable defined, and an explicit empty value is not an absent one.
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

	var cfg ConsumerConfig
	if err := env.Parse(&cfg); err != nil {
		t.Fatalf("env.Parse: %v", err)
	}
	if cfg.Concurrency != DefaultConcurrency {
		t.Errorf("Concurrency = %d, want %d", cfg.Concurrency, DefaultConcurrency)
	}
	if cfg.AckWait != DefaultAckWait {
		t.Errorf("AckWait = %v, want %v", cfg.AckWait, DefaultAckWait)
	}
	if cfg.MaxDeliver != DefaultMaxDeliver {
		t.Errorf("MaxDeliver = %d, want %d", cfg.MaxDeliver, DefaultMaxDeliver)
	}
}
