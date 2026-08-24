package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const natsImage = "nats:2.11.4"

// ResolverSuite exercises kvCallbackResolver against a real NATS JetStream KV
// store, covering the path that the fake-resolver unit tests cannot.
type ResolverSuite struct {
	suite.Suite
	natsC    testcontainers.Container
	nc       *nats.Conn
	kv       jetstream.KeyValue
	resolver *kvCallbackResolver
}

func (s *ResolverSuite) SetupSuite() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        natsImage,
		ExposedPorts: []string{"4222/tcp", "8222/tcp"},
		Cmd:          []string{"-js", "-m", "8222"},
		WaitingFor:   wait.ForHTTP("/healthz").WithPort("8222/tcp"),
	}
	var err error
	s.natsC, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(s.T(), err)

	endpoint, err := s.natsC.Endpoint(ctx, "")
	require.NoError(s.T(), err)

	s.nc, err = nats.Connect("nats://" + endpoint)
	require.NoError(s.T(), err)

	js, err := jetstream.New(s.nc)
	require.NoError(s.T(), err)

	s.kv, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "HADES_JOBS"})
	require.NoError(s.T(), err)

	s.resolver = newKVCallbackResolver(s.kv)
}

func (s *ResolverSuite) TearDownSuite() {
	if s.nc != nil {
		s.nc.Close()
	}
	if s.natsC != nil {
		if err := s.natsC.Terminate(context.Background()); err != nil {
			slog.Error("Could not stop NATS", "error", err)
		}
	}
}

func (s *ResolverSuite) putJob(job payload.QueuePayload) {
	data, err := json.Marshal(job)
	require.NoError(s.T(), err)
	_, err = s.kv.Put(context.Background(), job.ID.String(), data)
	require.NoError(s.T(), err)
}

func (s *ResolverSuite) TestResolvesStoredCallbackURL() {
	job := payload.QueuePayload{ID: uuid.New(), Name: "job", CallbackURL: "https://example.com/adapter/logs"}
	s.putJob(job)

	url, err := s.resolver.CallbackURL(context.Background(), job.ID.String())
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/adapter/logs", url)
}

func (s *ResolverSuite) TestJobWithoutCallbackURL() {
	job := payload.QueuePayload{ID: uuid.New(), Name: "job"}
	s.putJob(job)

	url, err := s.resolver.CallbackURL(context.Background(), job.ID.String())
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "", url)
}

func (s *ResolverSuite) TestMissingKeyIsNotAnError() {
	url, err := s.resolver.CallbackURL(context.Background(), uuid.New().String())
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "", url)
}

func (s *ResolverSuite) TestResolvesStatusCallbackURL() {
	job := payload.QueuePayload{
		ID:                uuid.New(),
		Name:              "status job",
		CallbackURL:       "https://example.com/adapter/logs",
		StatusCallbackURL: "https://example.com/hades/status",
	}
	s.putJob(job)

	info, err := s.resolver.JobInfo(context.Background(), job.ID.String())
	require.NoError(s.T(), err)
	assert.True(s.T(), info.Found)
	assert.Equal(s.T(), "status job", info.Name)
	assert.Equal(s.T(), "https://example.com/hades/status", info.StatusCallbackURL)
}

func (s *ResolverSuite) TestJobInfoWithoutStatusCallbackURL() {
	job := payload.QueuePayload{ID: uuid.New(), Name: "logs only", CallbackURL: "https://example.com/adapter/logs"}
	s.putJob(job)

	info, err := s.resolver.JobInfo(context.Background(), job.ID.String())
	require.NoError(s.T(), err)
	assert.True(s.T(), info.Found)
	assert.Equal(s.T(), "", info.StatusCallbackURL, "log forwarding must not double as a status webhook")
}

func (s *ResolverSuite) TestJobInfoForMissingKeyIsNotAnError() {
	info, err := s.resolver.JobInfo(context.Background(), uuid.New().String())
	require.NoError(s.T(), err)
	assert.False(s.T(), info.Found)
}

func TestResolverSuite(t *testing.T) {
	suite.Run(t, new(ResolverSuite))
}
