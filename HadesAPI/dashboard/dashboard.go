// Package dashboard adds an authenticated, live operator UI on top of the
// existing HadesAPI Gin server. It exposes a small JSON + SSE API under /api and
// serves the embedded single-page app; no separate service is introduced.
//
// Data sources (all already reachable from HadesAPI):
//   - Job identity/priority: recorded directly at enqueue time via TrackEnqueue
//     (the KV payload does not carry the submission priority).
//   - Job status + timing: a live subscription to the core-NATS
//     "hades.jobstatus.*" subjects. The tracker is never seeded from durable
//     storage, so after a restart it shows jobs as it re-learns them live -
//     the same ephemeral model as HadesLogManager - rather than mislabeling
//     everything as Queued.
//   - Full job definition (detail view): read on demand from the "HADES_JOBS"
//     JetStream KV bucket and redacted before it leaves the process.
//   - Logs: proxied to the internal HadesLogManager service, which remains the
//     single log aggregator.
//   - Metrics: derived from the tracker plus approximate JetStream queue depth.
package dashboard

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	hadesnats "github.com/hades-scheduler/hades/shared/nats"
	"github.com/hades-scheduler/hades/shared/redact"
	"github.com/hades-scheduler/hades/shared/utils"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// jobsStream is the JetStream stream carrying queued job references.
	jobsStream = "HADES_JOBS"
	// metricsBroadcastInterval is how often aggregate metrics are pushed to
	// connected SSE clients.
	metricsBroadcastInterval = 5 * time.Second
)

// Config holds the dashboard configuration, loaded from the environment. The
// dashboard is disabled (all /api routes return 503) unless Username,
// PasswordHash, and SessionSecret are all set - it never defaults to open.
type Config struct {
	Username      string        `env:"DASHBOARD_USERNAME"`
	PasswordHash  string        `env:"DASHBOARD_PASSWORD_HASH"`
	SessionSecret string        `env:"DASHBOARD_SESSION_SECRET"`
	SessionTTL    time.Duration `env:"DASHBOARD_SESSION_TTL" envDefault:"12h"`
	JobRetention  time.Duration `env:"DASHBOARD_JOB_RETENTION" envDefault:"1h"`
	LogManagerURL string        `env:"LOG_MANAGER_URL" envDefault:"http://hades-log-manager-service:8081"`
	// Version is the deployed build identifier (the container image tag), shown
	// in the dashboard header. Defaults to "dev" for local runs.
	Version string `env:"HADES_VERSION" envDefault:"dev"`
	// InsecureCookie drops the Secure flag on the session cookie. Only for local
	// HTTP development; never enable behind anything other than localhost.
	InsecureCookie bool `env:"DASHBOARD_COOKIE_INSECURE" envDefault:"false"`

	Redact redact.Config
}

// LoadConfig loads the dashboard configuration from the environment.
func LoadConfig() (Config, error) {
	var cfg Config
	if err := utils.LoadConfig(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Enabled reports whether the dashboard has the credentials required to run.
func (c Config) Enabled() bool {
	return c.Username != "" && c.PasswordHash != "" && c.SessionSecret != ""
}

// Server holds the dashboard's runtime state and dependencies.
type Server struct {
	cfg      Config
	nc       *nats.Conn
	js       jetstream.JetStream
	kv       jetstream.KeyValue
	redactor *redact.Redactor
	tracker  *tracker
	hub      *hub
	auth     *authenticator
	http     *http.Client
	sub      *nats.Subscription
}

// NewServer builds a dashboard Server. It opens a read handle on the job KV
// bucket. It returns an error if the configuration is invalid or the KV bucket
// cannot be opened. Callers should check cfg.Enabled() first; a disabled
// dashboard still constructs so its routes can return a clear 503.
func NewServer(ctx context.Context, cfg Config, nc *nats.Conn) (*Server, error) {
	redactor, err := redact.New(cfg.Redact)
	if err != nil {
		return nil, fmt.Errorf("building redactor: %w", err)
	}

	auth, err := newAuthenticator(cfg)
	if err != nil {
		return nil, fmt.Errorf("building authenticator: %w", err)
	}

	// When the dashboard is disabled it only needs to answer 503, so skip the
	// JetStream/KV setup entirely.
	var js jetstream.JetStream
	var kv jetstream.KeyValue
	if cfg.Enabled() {
		js, err = jetstream.New(nc)
		if err != nil {
			return nil, fmt.Errorf("creating JetStream context: %w", err)
		}
		kv, err = js.KeyValue(ctx, hadesnats.JobsBucket)
		if err != nil {
			return nil, fmt.Errorf("opening KV bucket %q: %w", hadesnats.JobsBucket, err)
		}
	}

	return &Server{
		cfg:      cfg,
		nc:       nc,
		js:       js,
		kv:       kv,
		redactor: redactor,
		tracker:  newTracker(cfg.JobRetention),
		hub:      newHub(),
		auth:     auth,
		http:     &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// Start subscribes to job status events and starts background loops (tracker
// cleanup, periodic metrics broadcast). It returns once the subscription is
// established; the loops run until ctx is cancelled.
func (s *Server) Start(ctx context.Context) error {
	sub, err := s.nc.Subscribe(buildstatus.StatusSubject("*"), func(msg *nats.Msg) {
		status := statusFromSubject(msg.Subject)
		jobID := string(msg.Data)
		if jobID == "" || !status.IsValid() {
			return
		}
		// An optional reason (e.g. why the job Failed) rides in a header; Get is
		// nil-safe when no header was set.
		reason := msg.Header.Get(buildstatus.ReasonHeader)
		summary := s.tracker.observe(jobID, status, reason)
		s.hub.broadcast(event{Type: eventJob, Job: summary})
	})
	if err != nil {
		return fmt.Errorf("subscribing to job status: %w", err)
	}
	s.sub = sub

	go s.tracker.cleanupLoop(ctx)
	go s.metricsLoop(ctx)

	go func() {
		<-ctx.Done()
		if err := s.sub.Drain(); err != nil {
			slog.Warn("Failed to drain dashboard status subscription", "error", err)
		}
		s.hub.closeAll()
	}()

	slog.Info("Dashboard started", "log_manager_url", s.cfg.LogManagerURL, "redact_mode", s.cfg.Redact.Mode)
	return nil
}

// Enabled reports whether the dashboard is configured to serve.
func (s *Server) Enabled() bool { return s.cfg.Enabled() }

// TrackEnqueue records a freshly enqueued job together with its name and
// priority, which the stored KV payload does not carry (priority) or which
// saves a KV read (name). Called by the API on a successful enqueue so queued
// jobs appear immediately.
func (s *Server) TrackEnqueue(jobID, name string, stepCount int, priority hades.Priority) {
	summary := s.tracker.enqueue(jobID, name, stepCount, priority)
	s.hub.broadcast(event{Type: eventJob, Job: summary})
}

// metricsLoop periodically pushes an aggregate metrics snapshot to SSE clients.
func (s *Server) metricsLoop(ctx context.Context) {
	ticker := time.NewTicker(metricsBroadcastInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Metrics cost three JetStream round-trips and a tracker scan; with
			// nobody watching they would be computed only to be thrown away.
			if s.hub.clientCount() == 0 {
				continue
			}
			s.hub.broadcast(event{Type: eventMetrics, Metrics: s.metrics(ctx)})
		}
	}
}
