package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hades-scheduler/hades/hadesAPI/dashboard"
	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/payload"
	"golang.org/x/crypto/bcrypt"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	hadesnats "github.com/hades-scheduler/hades/shared/nats"
)

const NATS_IMAGE = "nats:2.11.4"

type APISuite struct {
	suite.Suite
	router          *gin.Engine
	natsC           testcontainers.Container
	natsConnection  *nats.Conn
	hadesProducer   hades.JobPublisher
	statusPublisher buildstatus.StatusPublisher
}

func (suite *APISuite) SetupSuite() {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        NATS_IMAGE,
		ExposedPorts: []string{"4222/tcp", "8222/tcp"},
		Cmd:          []string{"-js", "-m", "8222"},
		WaitingFor:   wait.ForHTTP("/healthz").WithPort("8222/tcp"),
	}
	var err error
	suite.natsC, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		slog.Error("Could not start NATS", "error", err)
	}

	endpoint, err := suite.natsC.Endpoint(ctx, "")
	slog.Info("NATS endpoint", "endpoint", endpoint)
	if err != nil {
		slog.Error("Could not get NATS endpoint", "error", err)
	}

	// Setup NATS connection
	natsConfig := hadesnats.ConnectionConfig{
		URL:      "nats://" + endpoint,
		Username: "",
		Password: "",
	}

	suite.natsConnection, err = hadesnats.SetupDefaultNatsConnection(natsConfig)
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
	}

	// Create producer for tests. HadesNATSPublisher implements both the job
	// publisher and the status publisher, so it serves as both dependencies.
	producer, err := hadesnats.NewHadesPublisher(suite.natsConnection)
	if err != nil {
		slog.Error("Failed to create HadesProducer", "error", err)
	}
	suite.hadesProducer = producer
	suite.statusPublisher = producer

	suite.router = setupRouter("", suite.hadesProducer, suite.statusPublisher, nil)
}

func (suite *APISuite) TearDownSuite() {
	// Close NATS connection
	if suite.natsConnection != nil {
		suite.natsConnection.Close()
	}

	// Stop NATS container
	ctx := context.Background()
	if err := suite.natsC.Terminate(ctx); err != nil {
		slog.Error("Could not stop NATS", "error", err)
	}
}

func (suite *APISuite) TestPingRoute() {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ping", nil)
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), 200, w.Code)

	var body map[string]string
	assert.NoError(suite.T(), json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(suite.T(), "ok", body["status"])
	_, err := time.Parse(time.RFC3339, body["timestamp"])
	assert.NoError(suite.T(), err)
}

func (suite *APISuite) TestAuthBoundary() {
	router := setupRouter("secret", suite.hadesProducer, suite.statusPublisher, nil)

	// /ping is outside the auth group and must stay open
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ping", nil)
	router.ServeHTTP(w, req)
	assert.Equal(suite.T(), 200, w.Code)
	var body map[string]string
	assert.NoError(suite.T(), json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(suite.T(), "ok", body["status"])
	_, err := time.Parse(time.RFC3339, body["timestamp"])
	assert.NoError(suite.T(), err)

	// POST /build without credentials must be rejected
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/build", nil)
	router.ServeHTTP(w, req)
	assert.Equal(suite.T(), 401, w.Code)
}

func (suite *APISuite) TestNoAuthBoundary() {
	router := setupRouter("", suite.hadesProducer, suite.statusPublisher, nil)

	// /ping must still return 200
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ping", nil)
	router.ServeHTTP(w, req)
	assert.Equal(suite.T(), 200, w.Code)
	var body map[string]string
	assert.NoError(suite.T(), json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(suite.T(), "ok", body["status"])
	_, err := time.Parse(time.RFC3339, body["timestamp"])
	assert.NoError(suite.T(), err)

	// POST /build without credentials must not be rejected by auth (no 401)
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/build", nil)
	router.ServeHTTP(w, req)
	assert.NotEqual(suite.T(), 401, w.Code)
}

func (suite *APISuite) TestAddBuildToQueueRoute() {
	w := httptest.NewRecorder()
	restPayload := payload.RESTPayload{
		Priority: 1,
		QueuePayload: payload.QueuePayload{
			Name:      "example",
			Timestamp: time.Now(),
			Metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			Steps: []payload.Step{
				{
					ID:          1,
					Name:        "step1",
					Image:       "image1",
					Script:      "script1",
					Metadata:    map[string]string{},
					CPULimit:    1,
					MemoryLimit: "1G",
				},
				{
					ID:          2,
					Name:        "step2",
					Image:       "image2",
					Script:      "script2",
					Metadata:    map[string]string{},
					CPULimit:    2,
					MemoryLimit: "2G",
				},
			},
		},
	}
	jsonValue, _ := json.Marshal(restPayload)
	req, _ := http.NewRequest("POST", "/build", bytes.NewBuffer(jsonValue))
	req.Header.Set("Content-Type", "application/json")
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), 200, w.Code)
}

func (suite *APISuite) TestInvalidMemoryLimit() {
	w := httptest.NewRecorder()
	restPayload := payload.RESTPayload{
		Priority: 1,
		QueuePayload: payload.QueuePayload{
			Name:      "example",
			Timestamp: time.Now(),
			Steps: []payload.Step{
				{
					ID:          1,
					Name:        "step1",
					Image:       "image1",
					Script:      "script1",
					Metadata:    map[string]string{},
					CPULimit:    1,
					MemoryLimit: "1XXXX",
				},
			},
		},
	}
	jsonValue, _ := json.Marshal(restPayload)
	req, _ := http.NewRequest("POST", "/build", bytes.NewBuffer(jsonValue))
	req.Header.Set("Content-Type", "application/json")
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), 400, w.Code)
	assert.Equal(suite.T(), "Failed to parse RAM limit", w.Body.String())
}

// postStep is a helper that submits a single-step job and returns the response.
func (suite *APISuite) postStep(step payload.Step, timeoutSeconds int64) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	restPayload := payload.RESTPayload{
		Priority: 1,
		QueuePayload: payload.QueuePayload{
			Name:           "example",
			Timestamp:      time.Now(),
			TimeoutSeconds: timeoutSeconds,
			Steps:          []payload.Step{step},
		},
	}
	jsonValue, _ := json.Marshal(restPayload)
	req, _ := http.NewRequest("POST", "/build", bytes.NewBuffer(jsonValue))
	req.Header.Set("Content-Type", "application/json")
	suite.router.ServeHTTP(w, req)
	return w
}

func (suite *APISuite) TestInvalidNetworkMode() {
	w := suite.postStep(payload.Step{ID: 1, Name: "s", Image: "image1", Network: "bad net"}, 0)
	assert.Equal(suite.T(), 400, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "network")
}

func (suite *APISuite) TestMemorySwapRequiresMemoryLimit() {
	w := suite.postStep(payload.Step{ID: 1, Name: "s", Image: "image1", MemorySwap: "2G"}, 0)
	assert.Equal(suite.T(), 400, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "memory_swap requires memory_limit")
}

func (suite *APISuite) TestMemorySwapSmallerThanMemoryLimit() {
	w := suite.postStep(payload.Step{ID: 1, Name: "s", Image: "image1", MemoryLimit: "2G", MemorySwap: "1G"}, 0)
	assert.Equal(suite.T(), 400, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "memory_swap must be greater than or equal to memory_limit")
}

func (suite *APISuite) TestNegativeTimeoutSeconds() {
	w := suite.postStep(payload.Step{ID: 1, Name: "s", Image: "image1"}, -5)
	assert.Equal(suite.T(), 400, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "timeout_seconds")
}

func (suite *APISuite) TestTimeoutSecondsTooLarge() {
	w := suite.postStep(payload.Step{ID: 1, Name: "s", Image: "image1"}, payload.MaxTimeoutSeconds+1)
	assert.Equal(suite.T(), 400, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "timeout_seconds")
}

func (suite *APISuite) TestValidCallbackURL() {
	w := httptest.NewRecorder()
	restPayload := payload.RESTPayload{
		Priority: 1,
		QueuePayload: payload.QueuePayload{
			Name:        "example",
			Timestamp:   time.Now(),
			CallbackURL: "https://example.com/adapter/logs",
			Steps:       []payload.Step{{ID: 1, Name: "step1", Image: "image1", Script: "script1"}},
		},
	}
	jsonValue, _ := json.Marshal(restPayload)
	req, _ := http.NewRequest("POST", "/build", bytes.NewBuffer(jsonValue))
	req.Header.Set("Content-Type", "application/json")
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), 200, w.Code)
}

func (suite *APISuite) TestInvalidCallbackURL() {
	w := httptest.NewRecorder()
	restPayload := payload.RESTPayload{
		Priority: 1,
		QueuePayload: payload.QueuePayload{
			Name:        "example",
			Timestamp:   time.Now(),
			CallbackURL: "not-a-url",
			Steps:       []payload.Step{{ID: 1, Name: "step1", Image: "image1", Script: "script1"}},
		},
	}
	jsonValue, _ := json.Marshal(restPayload)
	req, _ := http.NewRequest("POST", "/build", bytes.NewBuffer(jsonValue))
	req.Header.Set("Content-Type", "application/json")
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), 400, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "Invalid callback_url")
}

func (suite *APISuite) TestValidStatusCallbackURL() {
	w := httptest.NewRecorder()
	restPayload := payload.RESTPayload{
		Priority: 1,
		QueuePayload: payload.QueuePayload{
			Name:              "example",
			Timestamp:         time.Now(),
			CallbackURL:       "https://example.com/adapter/logs",
			StatusCallbackURL: "https://example.com/hades/status",
			Steps:             []payload.Step{{ID: 1, Name: "step1", Image: "image1", Script: "script1"}},
		},
	}
	jsonValue, _ := json.Marshal(restPayload)
	req, _ := http.NewRequest("POST", "/build", bytes.NewBuffer(jsonValue))
	req.Header.Set("Content-Type", "application/json")
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), 200, w.Code)
}

func (suite *APISuite) TestInvalidStatusCallbackURL() {
	w := httptest.NewRecorder()
	restPayload := payload.RESTPayload{
		Priority: 1,
		QueuePayload: payload.QueuePayload{
			Name:              "example",
			Timestamp:         time.Now(),
			StatusCallbackURL: "ftp://example.com/status",
			Steps:             []payload.Step{{ID: 1, Name: "step1", Image: "image1", Script: "script1"}},
		},
	}
	jsonValue, _ := json.Marshal(restPayload)
	req, _ := http.NewRequest("POST", "/build", bytes.NewBuffer(jsonValue))
	req.Header.Set("Content-Type", "application/json")
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), 400, w.Code)
	assert.Contains(suite.T(), w.Body.String(), "Invalid status_callback_url")
}

func (suite *APISuite) TestInvalidJSON() {
	w := httptest.NewRecorder()
	restPayload := struct {
		Priority int    `json:"priority"`
		TaskName string `json:"task_name"`
	}{
		Priority: 1,
		TaskName: "example",
	}
	jsonValue, _ := json.Marshal(restPayload)
	req, _ := http.NewRequest("POST", "/build", bytes.NewBuffer(jsonValue))
	req.Header.Set("Content-Type", "application/json")
	suite.router.ServeHTTP(w, req)

	assert.Equal(suite.T(), 400, w.Code)
	assert.Equal(suite.T(), `Invalid request payload: "name" is required`, w.Body.String())
}

// TestDashboardEndToEnd exercises the enabled dashboard against the real NATS
// container: it logs in, submits a job carrying sensitive metadata, then reads
// it back through /api/jobs and /api/jobs/:id and asserts secrets are redacted.
func (suite *APISuite) TestDashboardEndToEnd() {
	t := suite.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hash, err := bcrypt.GenerateFromPassword([]byte("pw12345"), bcrypt.MinCost)
	assert.NoError(t, err)
	dashCfg := dashboard.Config{
		Username:      "admin",
		PasswordHash:  string(hash),
		SessionSecret: "0123456789abcdef-0123456789abcdef-secret",
		JobRetention:  time.Hour,
		LogManagerURL: "http://127.0.0.1:0",
	}
	dash, err := dashboard.NewServer(ctx, dashCfg, suite.natsConnection)
	assert.NoError(t, err)
	assert.NoError(t, dash.Start(ctx))

	router := setupRouter("", suite.hadesProducer, suite.statusPublisher, dash)

	// Log in and capture the session cookie.
	loginBody := `{"username":"admin","password":"pw12345"}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/login", bytes.NewBufferString(loginBody))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var cookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == "hades_dashboard_session" {
			cookie = c
		}
	}
	assert.NotNil(t, cookie)

	// Submit a job with a visible key and two secret-bearing values.
	job := payload.RESTPayload{
		Priority: 3,
		QueuePayload: payload.QueuePayload{
			Name:      "secret-job",
			Timestamp: time.Now(),
			Metadata: map[string]string{
				"REPO_URL":     "https://github.com/org/repo.git",
				"GIT_PASSWORD": "supersecret",
				"DATABASE_URL": "postgres://user:pass@db:5432/app",
			},
			Steps: []payload.Step{{
				ID:     1,
				Name:   "s1",
				Image:  "alpine",
				Script: "git clone https://u:scriptsecret123456@github.com/x/y.git && export API_KEY=sk-live-leakme",
			}},
		},
	}
	jsonValue, _ := json.Marshal(job)
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("POST", "/build", bytes.NewBuffer(jsonValue))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	var buildResp map[string]string
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &buildResp))
	jobID := buildResp["job_id"]
	assert.NotEmpty(t, jobID)

	// The job shows up in the list (tracked synchronously at enqueue).
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/api/jobs", nil)
	req.AddCookie(cookie)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	assert.Contains(t, w.Body.String(), jobID)

	// Detail view redacts secret values but keeps the innocuous one.
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/api/jobs/"+jobID, nil)
	req.AddCookie(cookie)
	router.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "https://github.com/org/repo.git") // REPO_URL visible
	assert.NotContains(t, body, "supersecret")                  // GIT_PASSWORD masked
	assert.NotContains(t, body, "user:pass@db")                 // DATABASE_URL masked
	assert.NotContains(t, body, "scriptsecret123456")           // secret in step script masked
	assert.NotContains(t, body, "sk-live-leakme")               // API key in step script masked

	// Unauthenticated access is rejected.
	w = httptest.NewRecorder()
	req, _ = http.NewRequest("GET", "/api/jobs", nil)
	router.ServeHTTP(w, req)
	assert.Equal(t, 401, w.Code)
}

// TestDashboardLogStream exercises the live log SSE endpoint against real NATS
// JetStream: it publishes a log to hades.logs.<jobID>, then connects to
// /api/jobs/:id/logs/stream and asserts the log is delivered as a "log" SSE
// frame (DeliverAllPolicy replays the retained backlog to the new subscriber).
func (suite *APISuite) TestDashboardLogStream() {
	t := suite.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hash, err := bcrypt.GenerateFromPassword([]byte("pw12345"), bcrypt.MinCost)
	require.NoError(t, err)
	dashCfg := dashboard.Config{
		Username:      "admin",
		PasswordHash:  string(hash),
		SessionSecret: "0123456789abcdef-0123456789abcdef-secret",
		JobRetention:  time.Hour,
		LogManagerURL: "http://127.0.0.1:0",
	}
	dash, err := dashboard.NewServer(ctx, dashCfg, suite.natsConnection)
	require.NoError(t, err)
	require.NoError(t, dash.Start(ctx))

	router := setupRouter("", suite.hadesProducer, suite.statusPublisher, dash)
	srv := httptest.NewServer(router)
	defer srv.Close()

	// Log in and capture the session cookie.
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/login", bytes.NewBufferString(`{"username":"admin","password":"pw12345"}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	var cookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == "hades_dashboard_session" {
			cookie = c
		}
	}
	require.NotNil(t, cookie)

	// Publish a log to the job's subject before connecting; DeliverAllPolicy
	// replays it to the new subscriber.
	producer, err := buildlogs.NewHadesLogProducer(suite.natsConnection)
	require.NoError(t, err)
	jobID := uuid.NewString()
	require.NoError(t, producer.PublishJobLog(ctx, buildlogs.Log{
		JobID:       jobID,
		ContainerID: "step-1",
		Logs:        []buildlogs.LogEntry{{Timestamp: time.Now(), Message: "hello-live-log", OutputStream: "stdout"}},
	}))

	// Connect to the live log stream.
	streamReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/jobs/"+jobID+"/logs/stream", nil)
	streamReq.AddCookie(cookie)
	resp, err := http.DefaultClient.Do(streamReq)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	reader := bufio.NewReader(resp.Body)
	got := make(chan string, 1)
	go func() {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				got <- ""
				return
			}
			if strings.HasPrefix(line, "data: ") && strings.Contains(line, "hello-live-log") {
				got <- line
				return
			}
		}
	}()

	select {
	case line := <-got:
		require.Contains(t, line, "hello-live-log")
		require.Contains(t, line, `"type":"log"`)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for live log SSE frame")
	}
}

func (suite *APISuite) TestSecurityHeaders() {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/ping", nil)
	suite.router.ServeHTTP(w, req)
	assert.Equal(suite.T(), "nosniff", w.Header().Get("X-Content-Type-Options"))
	assert.Equal(suite.T(), "DENY", w.Header().Get("X-Frame-Options"))
	assert.Contains(suite.T(), w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'")
	assert.Contains(suite.T(), w.Header().Get("Content-Security-Policy"), "default-src 'self'")
}

func TestAPISuite(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	suite.Run(t, new(APISuite))
}
