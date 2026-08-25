package nats

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAckProgressInterval(t *testing.T) {
	tests := []struct {
		name    string
		ackWait time.Duration
		want    time.Duration
	}{
		{"default ack wait is capped", DefaultAckWait, maxAckProgressInterval},
		{"long ack wait is capped", 1 * time.Hour, maxAckProgressInterval},
		{"short ack wait scales down", 3 * time.Second, 1 * time.Second},
		{"tiny ack wait hits the floor", 10 * time.Millisecond, minAckProgressInterval},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ackProgressInterval(tt.ackWait))
			// The heartbeat must always be well below AckWait, otherwise it
			// cannot reset the ack timer in time.
			if tt.ackWait > minAckProgressInterval {
				assert.Less(t, ackProgressInterval(tt.ackWait), tt.ackWait)
			}
		})
	}
}

func TestConsumerConfigWithDefaults(t *testing.T) {
	cfg := ConsumerConfig{}.withDefaults()
	assert.Equal(t, uint(1), cfg.Concurrency)
	assert.Equal(t, DefaultAckWait, cfg.AckWait)
	assert.Equal(t, DefaultMaxDeliver, cfg.MaxDeliver)

	// MaxDeliver must never end up unlimited (0), which is the JetStream default.
	assert.Positive(t, ConsumerConfig{MaxDeliver: -1}.withDefaults().MaxDeliver)

	// Explicit values are kept.
	custom := ConsumerConfig{Concurrency: 4, AckWait: 5 * time.Second, MaxDeliver: 2}.withDefaults()
	assert.Equal(t, ConsumerConfig{Concurrency: 4, AckWait: 5 * time.Second, MaxDeliver: 2}, custom)
}

// TestConsumerConfigValidate pins the MaxDeliver lower bound: because the last
// allowed delivery reports the terminal failure instead of running the job,
// MaxDeliver = 1 would silently stop the scheduler from ever executing a job.
// That must fail loudly at startup instead.
func TestConsumerConfigValidate(t *testing.T) {
	err := ConsumerConfig{MaxDeliver: 1}.withDefaults().validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NATS_MAX_DELIVER")

	// Unset (zero/negative) means "use the default", not "one delivery".
	assert.NoError(t, ConsumerConfig{}.withDefaults().validate())
	assert.NoError(t, ConsumerConfig{MaxDeliver: -1}.withDefaults().validate())

	// The smallest usable value still allows one execution attempt.
	assert.NoError(t, ConsumerConfig{MaxDeliver: minMaxDeliver}.withDefaults().validate())
}

// TestConsumerConfigValidateAckWait pins the AckWait lower bound. The heartbeat
// targets AckWait/3, giving three ticks per ack window so a delayed or dropped
// one is harmless. Below 3*minAckProgressInterval the clamp wins and that
// margin collapses towards a single tick; under minAckProgressInterval it is
// gone altogether and the first heartbeat lands after AckWait has elapsed,
// redelivering a job that is still running.
func TestConsumerConfigValidateAckWait(t *testing.T) {
	err := ConsumerConfig{AckWait: 10 * time.Millisecond}.withDefaults().validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NATS_ACK_WAIT")

	// Unset (zero/negative) means "use the default", not "expire immediately".
	assert.NoError(t, ConsumerConfig{}.withDefaults().validate())
	assert.NoError(t, ConsumerConfig{AckWait: -1}.withDefaults().validate())

	// At the bound the heartbeat is exactly minAckProgressInterval, which still
	// leaves two further heartbeats before AckWait elapses.
	assert.NoError(t, ConsumerConfig{AckWait: minAckWait}.withDefaults().validate())
	assert.Equal(t, minAckProgressInterval, ackProgressInterval(minAckWait))

	// Just below it, the clamp holds the interval above AckWait/3 and the
	// three-ticks-per-window margin starts to erode.
	assert.Error(t, ConsumerConfig{AckWait: minAckWait - time.Millisecond}.withDefaults().validate())
}

func TestStartAckProgressSignalsRepeatedly(t *testing.T) {
	var calls atomic.Int64
	stop := startAckProgress(func() error {
		calls.Add(1)
		return nil
	}, 10*time.Millisecond, "job-1")

	assert.Eventually(t, func() bool { return calls.Load() >= 3 }, 2*time.Second, 5*time.Millisecond,
		"expected repeated in-progress signals while the job is running")

	stop()
}

// TestStartAckProgressStopIsFinalAndIdempotent guards the two failure modes of
// the heartbeat goroutine: leaking past the job, and signalling in-progress on a
// message that has already been ACKed or NAKed.
func TestStartAckProgressStopIsFinalAndIdempotent(t *testing.T) {
	var calls atomic.Int64
	stop := startAckProgress(func() error {
		calls.Add(1)
		return nil
	}, time.Millisecond, "job-1")

	assert.Eventually(t, func() bool { return calls.Load() > 0 }, 2*time.Second, time.Millisecond)

	stop()
	after := calls.Load()

	// Calling stop twice (defer plus explicit call in processJob) must not panic
	// on a closed channel or block.
	require.NotPanics(t, stop)

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, after, calls.Load(), "in-progress must not be signalled after stop returns")
}

func TestStartAckProgressSurvivesSignalErrors(t *testing.T) {
	var calls atomic.Int64
	stop := startAckProgress(func() error {
		calls.Add(1)
		return errors.New("connection closed")
	}, time.Millisecond, "job-1")
	defer stop()

	// A failed heartbeat is logged, not fatal: the next tick tries again.
	assert.Eventually(t, func() bool { return calls.Load() >= 3 }, 2*time.Second, time.Millisecond)
}
