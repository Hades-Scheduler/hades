package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hades-scheduler/hades/hadesAPI/dashboard"
	"github.com/hades-scheduler/hades/shared/metrics"
	hadesnats "github.com/hades-scheduler/hades/shared/nats"
	"github.com/hades-scheduler/hades/shared/timing"
	"github.com/hades-scheduler/hades/shared/utils"
	"github.com/prometheus/client_golang/prometheus"
)

// HadesAPIConfig holds the API server configuration. An empty AuthKey disables
// Basic Auth on the /build endpoint.
type HadesAPIConfig struct {
	APIPort     uint `env:"API_PORT,notEmpty" envDefault:"8080"`
	MetricsPort uint `env:"METRICS_PORT,notEmpty" envDefault:"8082"`
	NatsConfig  hadesnats.ConnectionConfig
	AuthKey     string `env:"AUTH_KEY"`
	// TrustedProxies are the CIDRs/IPs of front proxies (e.g. the ingress) that
	// may set X-Forwarded-For. Empty (default) trusts none, so ClientIP() uses
	// the direct RemoteAddr and cannot be spoofed via a forged XFF header - which
	// the dashboard login lockout relies on. Set this to your ingress' address
	// range when deployed behind a reverse proxy.
	TrustedProxies []string `env:"DASHBOARD_TRUSTED_PROXIES" envSeparator:","`
}

var cfg HadesAPIConfig

// @title                      HadesAPI
// @version                    1.0
// @description                Job-submission API for Hades, a scalable scheduler for containerized jobs. Submit a multi-step job and it is validated, assigned a UUID, and queued on NATS by priority for the scheduler to execute.
// @BasePath                   /
// @securityDefinitions.basic  BasicAuth
func main() {
	utils.SetupLogging()

	if err := utils.LoadConfig(&cfg); err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	slog.Info("HadesAPI configuration",
		"api_port", cfg.APIPort,
		"metrics_port", cfg.MetricsPort,
		"nats_url", cfg.NatsConfig.URL,
		"nats_tls", cfg.NatsConfig.TLS,
		"auth_enabled", cfg.AuthKey != "",
	)

	natsConn, err := hadesnats.SetupDefaultNatsConnection(cfg.NatsConfig)
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer natsConn.Close()

	producer, err := hadesnats.NewHadesPublisher(natsConn)
	if err != nil {
		slog.Error("Failed to create HadesProducer", "error", err)
		os.Exit(1)
	}

	// Dashboard lifecycle context, cancelled on shutdown to stop its NATS
	// subscription and background loops.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Expose the phase-timing histograms on the default registry that
	// metrics.Serve scrapes, and enable tracing (noop unless an OTLP endpoint is
	// configured). The API opens each job's root span in addBuildToQueue.
	timing.MustRegister(prometheus.DefaultRegisterer)
	tracingShutdown, err := timing.InitTracing(ctx, "hades-api")
	if err != nil {
		slog.Error("Failed to init tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tracingShutdown(shutdownCtx)
	}()

	dashCfg, err := dashboard.LoadConfig()
	if err != nil {
		slog.Error("Failed to load dashboard configuration", "error", err)
		os.Exit(1)
	}
	// The dashboard is always constructed so its /api routes can return a clear
	// 503 when it is not configured; it only subscribes to NATS and serves the
	// SPA once credentials are present.
	dash, err := dashboard.NewServer(ctx, dashCfg, natsConn)
	if err != nil {
		slog.Error("Failed to create dashboard", "error", err)
		os.Exit(1)
	}
	if dash.Enabled() {
		if err := dash.Start(ctx); err != nil {
			slog.Error("Failed to start dashboard", "error", err)
			os.Exit(1)
		}
		slog.Info("Dashboard enabled")
	} else {
		slog.Warn("Dashboard disabled (set DASHBOARD_USERNAME, DASHBOARD_PASSWORD_HASH and DASHBOARD_SESSION_SECRET to enable)")
	}

	slog.Info("Starting HadesAPI on port", "port", cfg.APIPort)
	gin.SetMode(gin.ReleaseMode)

	r := setupRouter(cfg.AuthKey, producer, producer, dash)

	// Only trust X-Forwarded-For from explicitly configured proxies; otherwise
	// ClientIP() falls back to the un-spoofable RemoteAddr. This keeps the login
	// lockout keyed on a value the client cannot forge.
	if err := r.SetTrustedProxies(cfg.TrustedProxies); err != nil {
		slog.Error("Failed to set trusted proxies", "error", err)
		os.Exit(1)
	}

	server := &http.Server{
		Addr:        fmt.Sprintf(":%d", cfg.APIPort),
		Handler:     r,
		ReadTimeout: 15 * time.Second,
		// WriteTimeout is intentionally disabled: the dashboard SSE endpoint
		// (/api/stream) is a long-lived response that a non-zero server-wide
		// WriteTimeout would sever. Reads are still bounded by ReadTimeout and
		// ReadHeaderTimeout.
		WriteTimeout:      0,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Failed to start HadesAPI", "error", err)
			os.Exit(1)
		}
	}()

	// Prometheus metrics on a dedicated, cluster-internal port (not exposed via
	// the public ingress). Stops when the shutdown context is cancelled.
	go func() {
		// Metrics are auxiliary: log and keep serving the API rather than taking
		// it down (and skipping the deferred tracing/NATS shutdown that os.Exit
		// from a goroutine would bypass).
		if err := metrics.Serve(ctx, fmt.Sprintf(":%d", cfg.MetricsPort)); err != nil {
			slog.Error("Metrics server failed; continuing without metrics", "error", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	slog.Info("Received shutdown signal", "signal", sig.String())

	// Stop the dashboard's background work before draining HTTP.
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("Server shutdown error", "error", err)
	}
	slog.Info("HadesAPI shutdown complete")
}
