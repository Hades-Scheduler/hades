package controller

import (
	"testing"
	"time"
)

// A reconciler built without explicit tuning (as most tests build it) must use
// the defaults, and a misconfigured non-positive value must not reach
// ctrl.Result.RequeueAfter, where it would requeue immediately and busy-loop.
func TestReconcilerTuningFallsBackToDefaults(t *testing.T) {
	tests := []struct {
		name                string
		requeueDelay        time.Duration
		logDrainTimeout     time.Duration
		wantRequeueDelay    time.Duration
		wantLogDrainTimeout time.Duration
	}{
		{
			name:                "unset uses defaults",
			wantRequeueDelay:    DefaultRequeueDelay,
			wantLogDrainTimeout: DefaultLogDrainTimeout,
		},
		{
			name:                "negative uses defaults",
			requeueDelay:        -1 * time.Second,
			logDrainTimeout:     -1 * time.Second,
			wantRequeueDelay:    DefaultRequeueDelay,
			wantLogDrainTimeout: DefaultLogDrainTimeout,
		},
		{
			name:                "positive values are honoured",
			requeueDelay:        200 * time.Millisecond,
			logDrainTimeout:     90 * time.Second,
			wantRequeueDelay:    200 * time.Millisecond,
			wantLogDrainTimeout: 90 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &BuildJobReconciler{RequeueDelay: tt.requeueDelay, LogDrainTimeout: tt.logDrainTimeout}
			if got := r.requeueDelay(); got != tt.wantRequeueDelay {
				t.Errorf("requeueDelay() = %v, want %v", got, tt.wantRequeueDelay)
			}
			if got := r.logDrainTimeout(); got != tt.wantLogDrainTimeout {
				t.Errorf("logDrainTimeout() = %v, want %v", got, tt.wantLogDrainTimeout)
			}
		})
	}
}

// The documented defaults. RequeueDelay is only a safety resync because pod and
// log-stream events drive progress.
func TestDefaults(t *testing.T) {
	if DefaultRequeueDelay != 15*time.Second {
		t.Errorf("DefaultRequeueDelay = %v, want 15s", DefaultRequeueDelay)
	}
	if DefaultLogDrainTimeout != 45*time.Second {
		t.Errorf("DefaultLogDrainTimeout = %v, want 45s", DefaultLogDrainTimeout)
	}
}

func TestMaxConcurrentReconcilesFallsBackToDefault(t *testing.T) {
	for _, n := range []int{0, -3} {
		if got := (&BuildJobReconciler{MaxConcurrentReconciles: n}).maxConcurrentReconciles(); got != DefaultMaxConcurrentReconciles {
			t.Errorf("maxConcurrentReconciles() with %d = %d, want %d", n, got, DefaultMaxConcurrentReconciles)
		}
	}
	if got := (&BuildJobReconciler{MaxConcurrentReconciles: 3}).maxConcurrentReconciles(); got != 3 {
		t.Errorf("maxConcurrentReconciles() = %d, want 3", got)
	}
}
