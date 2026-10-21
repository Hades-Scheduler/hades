package nats

import (
	"context"
	"sync"
	"time"

	"github.com/hades-scheduler/hades/shared/payload"
)

// TestIdleConsumerPicksUpJobWithoutWaitingForBackoff checks that a job published
// to a fully backed-off consumer is picked up via the wake-up subscription
// rather than on the next poll, which can be up to a second away.
func (s *ConsumerRedeliverySuite) TestIdleConsumerPicksUpJobWithoutWaitingForBackoff() {
	got := make(chan struct{}, 1)
	stop := s.runConsumer(ConsumerConfig{Concurrency: 2, AckWait: testAckWait, MaxDeliver: DefaultMaxDeliver},
		func(job payload.QueuePayload) {
			select {
			case got <- struct{}{}:
			default:
			}
		})
	defer stop()

	// 10ms doubling reaches the 1s cap after ~7 empty polls (~1s in total); wait
	// well past that so the workers are sleeping on the longest interval.
	time.Sleep(3 * time.Second)

	start := time.Now()
	s.Require().NoError(s.publisher.EnqueueLowJob(context.Background(), newTestJob("idle-wake")))

	select {
	case <-got:
		s.Less(time.Since(start), 300*time.Millisecond, "idle consumer should be woken, not wait for the backoff poll")
	case <-time.After(5 * time.Second):
		s.FailNow("job was never picked up")
	}
}

// TestWakeKeepsStrictPriorityOrder queues one job per priority (low first) while
// the only worker is busy and expects them to run high, medium, low once it
// frees up. The wake-up must never let a lower priority job overtake.
func (s *ConsumerRedeliverySuite) TestWakeKeepsStrictPriorityOrder() {
	var mu sync.Mutex
	var order []string
	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan struct{})

	stop := s.runConsumer(ConsumerConfig{Concurrency: 1, AckWait: testAckWait, MaxDeliver: DefaultMaxDeliver},
		func(job payload.QueuePayload) {
			if job.Name == "blocker" {
				close(started)
				<-release
				return
			}
			mu.Lock()
			order = append(order, job.Name)
			n := len(order)
			mu.Unlock()
			if n == 3 {
				close(done)
			}
		})
	defer stop()

	ctx := context.Background()
	s.Require().NoError(s.publisher.EnqueueLowJob(ctx, newTestJob("blocker")))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		s.FailNow("blocker job never started")
	}

	s.Require().NoError(s.publisher.EnqueueLowJob(ctx, newTestJob("low")))
	s.Require().NoError(s.publisher.EnqueueMediumJob(ctx, newTestJob("medium")))
	s.Require().NoError(s.publisher.EnqueueHighJob(ctx, newTestJob("high")))
	close(release)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		s.FailNow("queued jobs were not all processed")
	}
	mu.Lock()
	defer mu.Unlock()
	s.Equal([]string{"high", "medium", "low"}, order)
}
