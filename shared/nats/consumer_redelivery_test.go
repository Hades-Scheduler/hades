package nats

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const natsImage = "nats:2.11.4"

// testAckWait is deliberately tiny so a "long-running" job in these tests only
// has to outlive a couple of seconds instead of the production AckWait.
const testAckWait = 2 * time.Second

// ConsumerRedeliverySuite exercises the JetStream redelivery behaviour of
// HadesNATSConsumer against a real NATS server.
type ConsumerRedeliverySuite struct {
	suite.Suite
	natsC     testcontainers.Container
	conn      *nats.Conn
	publisher *HadesNATSPublisher
}

func TestConsumerRedeliverySuite(t *testing.T) {
	suite.Run(t, new(ConsumerRedeliverySuite))
}

func (s *ConsumerRedeliverySuite) SetupSuite() {
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        natsImage,
			ExposedPorts: []string{"4222/tcp", "8222/tcp"},
			Cmd:          []string{"-js", "-m", "8222"},
			WaitingFor:   wait.ForHTTP("/healthz").WithPort("8222/tcp"),
		},
		Started: true,
	})
	s.Require().NoError(err)
	s.natsC = container

	endpoint, err := container.Endpoint(ctx, "")
	s.Require().NoError(err)

	s.conn, err = SetupDefaultNatsConnection(ConnectionConfig{URL: "nats://" + endpoint})
	s.Require().NoError(err)

	s.publisher, err = NewHadesPublisher(s.conn)
	s.Require().NoError(err)
}

func (s *ConsumerRedeliverySuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.natsC != nil {
		s.Require().NoError(testcontainers.TerminateContainer(s.natsC))
	}
}

// runConsumer starts the consumer's dequeue loop in the background and returns a
// function that stops it and waits for the workers to exit.
func (s *ConsumerRedeliverySuite) runConsumer(cfg ConsumerConfig, handler hades.PayloadHandler) func() {
	consumer, err := NewHadesConsumer(s.conn, cfg)
	s.Require().NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		consumer.DequeueJob(ctx, handler)
	}()

	return func() {
		cancel()
		<-done
	}
}

func newTestJob(name string) payload.QueuePayload {
	return payload.QueuePayload{
		ID:        uuid.New(),
		Name:      name,
		Timestamp: time.Now(),
		Steps:     []payload.Step{{ID: 1, Name: "step", Image: "alpine:latest"}},
	}
}

// TestLongRunningJobIsExecutedExactlyOnce is the regression test for the
// redelivery bug: the consumer used to ack only after the handler returned, so
// with the library's 30s AckWait every Docker-mode job longer than half a minute
// was redelivered and executed a second time while the first copy was still
// running - both copies sharing the same shared-<jobID> volume.
//
// The handler here blocks for several multiples of AckWait. With the in-progress
// heartbeat the job stays leased for as long as it runs, so it must be handed to
// a handler exactly once.
func (s *ConsumerRedeliverySuite) TestLongRunningJobIsExecutedExactlyOnce() {
	var started atomic.Int64
	finished := make(chan struct{})

	// Runs for 3x AckWait: without a heartbeat JetStream would have redelivered
	// the job twice by the time the handler returns.
	jobDuration := 3 * testAckWait

	stop := s.runConsumer(ConsumerConfig{Concurrency: 2, AckWait: testAckWait, MaxDeliver: DefaultMaxDeliver},
		func(job payload.QueuePayload) {
			if started.Add(1) == 1 {
				time.Sleep(jobDuration)
				close(finished)
			}
		})
	defer stop()

	job := newTestJob("long-running-job")
	s.Require().NoError(s.publisher.EnqueueHighJob(context.Background(), job))

	select {
	case <-finished:
	case <-time.After(jobDuration + 15*time.Second):
		s.FailNow("job handler never finished")
	}

	// Give JetStream more than a full AckWait to redeliver if the ack timer was
	// not being reset, plus time for a redelivered copy to reach the handler.
	time.Sleep(2 * testAckWait)

	s.Equal(int64(1), started.Load(), "a job running longer than AckWait must be executed exactly once")
}

// TestPoisonJobIsTerminatedAndReportedFailed covers the MaxDeliver backstop: a
// job that keeps killing its worker must stop being redelivered and must surface
// as Failed rather than disappearing silently.
func (s *ConsumerRedeliverySuite) TestPoisonJobIsTerminatedAndReportedFailed() {
	const maxDeliver = 2

	failures := make(chan *nats.Msg, 4)
	sub, err := s.conn.ChanSubscribe(buildstatus.StatusSubject(buildstatus.StatusFailed), failures)
	s.Require().NoError(err)
	defer func() { _ = sub.Unsubscribe() }()

	var attempts atomic.Int64
	stop := s.runConsumer(ConsumerConfig{Concurrency: 1, AckWait: testAckWait, MaxDeliver: maxDeliver},
		func(job payload.QueuePayload) {
			attempts.Add(1)
			panic("executor exploded")
		})
	defer stop()

	job := newTestJob("poison-job")
	s.Require().NoError(s.publisher.EnqueueHighJob(context.Background(), job))

	// Delivery 1 panics and is NAKed with a 5s delay; delivery 2 is the last
	// allowed one and is spent reporting the failure instead of running again.
	var failure *nats.Msg
	select {
	case failure = <-failures:
	case <-time.After(30 * time.Second):
		s.FailNow("no terminal Failed status was published for the poison job")
	}

	s.Equal(job.ID.String(), string(failure.Data))
	s.NotEmpty(failure.Header.Get(buildstatus.ReasonHeader), "the terminal failure must explain itself")
	s.Equal(int64(maxDeliver-1), attempts.Load(), "the final delivery must not execute the job again")

	// The message is terminated, so it must not come back.
	time.Sleep(3 * testAckWait)
	s.Equal(int64(maxDeliver-1), attempts.Load(), "a terminated job must not be redelivered")
	s.Empty(failures, "the terminal failure must be reported once")
}

// TearDownTest drops any job left over from a failed test so the next one starts
// from an empty work queue.
func (s *ConsumerRedeliverySuite) TearDownTest() {
	ctx := context.Background()
	js, err := jetstream.New(s.conn)
	s.Require().NoError(err)
	stream, err := js.Stream(ctx, "HADES_JOBS")
	s.Require().NoError(err)
	s.Require().NoError(stream.Purge(ctx))
}
