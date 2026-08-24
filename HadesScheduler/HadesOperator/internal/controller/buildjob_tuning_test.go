package controller

import (
	"testing"
	"time"
)

// A reconciler built without explicit tuning (as in the existing tests) must keep
// the historical timings, and a misconfigured non-positive value must not reach
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

// The historical hardcoded values are the defaults, so an operator that sets
// neither env var behaves exactly as before.
func TestDefaultsMatchHistoricalValues(t *testing.T) {
	if DefaultRequeueDelay != 2*time.Second {
		t.Errorf("DefaultRequeueDelay = %v, want 2s", DefaultRequeueDelay)
	}
	if DefaultLogDrainTimeout != 45*time.Second {
		t.Errorf("DefaultLogDrainTimeout = %v, want 45s", DefaultLogDrainTimeout)
	}
}
