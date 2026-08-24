package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/hades-scheduler/hades/hadesLogManager/docs" // generated OpenAPI spec (make docs-api)
	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/hades-scheduler/hades/shared/metrics"
	hadesnats "github.com/hades-scheduler/hades/shared/nats"
	"github.com/hades-scheduler/hades/shared/utils"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

const (
	shutdownTimeout = 30 * time.Second
)

// HadesLogManagerConfig holds the configuration for the log manager
type HadesLogManagerConfig struct {
	NatsConfig  hadesnats.ConnectionConfig
	APIPort     string `env:"HADESLOGMANAGER_API_PORT" envDefault:"8081"`
	MetricsPort string `env:"METRICS_PORT" envDefault:"8082"`
}

// @title                 HadesLogManager API
// @version               1.0
// @description           Read-only API for inspecting build-job logs and status aggregated by the Hades Log Manager. This service is a local-development aid and is not part of the production Helm deployment.
// @BasePath              /
func main() {
	// Setup logging
	utils.SetupLogging()
	gin.SetMode(gin.ReleaseMode)

	// Load configuration
	var cfg HadesLogManagerConfig
	if err := utils.LoadConfig(&cfg); err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	slog.Info("HadesLogManager configuration",
		"api_port", cfg.APIPort,
		"metrics_port", cfg.MetricsPort,
		"nats_url", cfg.NatsConfig.URL,
		"nats_tls", cfg.NatsConfig.TLS,
	)

	// Run main application
	if err := run(cfg); err != nil {
		slog.Error("Application error", "error", err)
		os.Exit(1)
	}
}

// run contains the main application logic with proper error handling
func run(cfg HadesLogManagerConfig) error {
	// Connect to NATS server
	nc, err := connectNATS(cfg.NatsConfig)
	if err != nil {
		return err
	}
	defer nc.Close()

	// Create log consumer
	consumer, err := buildlogs.NewHadesLogConsumer(nc)
	if err != nil {
		return err
	}

	// Create context for application lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Bind the HADES_JOBS KV store so the aggregator can resolve each job's
	// callback URL from its stored payload. CreateOrUpdate keeps startup
	// resilient to boot order relative to HadesAPI.
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream context: %w", err)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "HADES_JOBS"})
	if err != nil {
		return fmt.Errorf("binding HADES_JOBS KV store: %w", err)
	}
	resolver := newKVCallbackResolver(kv)

	// Create log aggregator
	var aggregatorConfig AggregatorConfig
	if err := utils.LoadConfig(&aggregatorConfig); err != nil {
		return fmt.Errorf("loading aggregator configuration: %w", err)
	}
	slog.Info("Log aggregator configuration",
		"batch_size", aggregatorConfig.BatchSize,
		"retention", aggregatorConfig.Retention,
		"max_job_logs", aggregatorConfig.MaxJobLogs,
	)
	logAggregator := NewLogAggregator(ctx, consumer, resolver, aggregatorConfig)

	// Create dynamic log manager
	dynamicManager := NewDynamicLogManager(nc, consumer, logAggregator)

	// Create the job-status webhook dispatcher. It is independent of the log
	// forwarding path above: it reacts to the terminal status event itself, so a
	// job's outcome is announced without waiting for the log consumer to drain.
	var webhookCfg StatusWebhookConfig
	if err := utils.LoadConfig(&webhookCfg); err != nil {
		return fmt.Errorf("loading status webhook configuration: %w", err)
	}
	// Normalize before logging, not just inside the dispatcher: an operator
	// reading this line during an incident must see the values actually in
	// effect. An invalid env var would otherwise be echoed back verbatim while
	// the dispatcher quietly ran on the repaired ones.
	webhookCfg = webhookCfg.normalized()
	slog.Info("Status webhook configuration",
		"enabled", webhookCfg.Enabled,
		"max_attempts", webhookCfg.MaxAttempts,
		"timeout", webhookCfg.Timeout,
		"initial_backoff", webhookCfg.InitialBackoff,
		"max_backoff", webhookCfg.MaxBackoff,
		"concurrency", webhookCfg.Concurrency,
		"max_pending", webhookCfg.MaxPending,
	)

	var dispatcher *StatusWebhookDispatcher
	if webhookCfg.Enabled {
		dispatcher = NewStatusWebhookDispatcher(js, resolver, nil, webhookCfg)
	}

	// Set up graceful shutdown
	return runWithGracefulShutdown(ctx, cancel, cfg, dynamicManager, logAggregator, dispatcher)
}

// connectNATS establishes connection to NATS server
func connectNATS(config hadesnats.ConnectionConfig) (*nats.Conn, error) {
	return hadesnats.SetupDefaultNatsConnection(config)
}

// runWithGracefulShutdown starts services and handles graceful shutdown
func runWithGracefulShutdown(ctx context.Context, cancel context.CancelFunc, cfg HadesLogManagerConfig, dynamicManager buildlogs.LogManager, logAggregator buildlogs.LogAggregator, dispatcher *StatusWebhookDispatcher,
) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 3)

	// Start the Prometheus metrics server on a dedicated, cluster-internal port.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := metrics.Serve(ctx, ":"+cfg.MetricsPort); err != nil {
			slog.Error("Metrics server failed", "error", err)
			errChan <- err
		}
	}()

	// Start the dynamic log manager
	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("Starting dynamic log manager")

		if err := dynamicManager.StartListening(ctx); err != nil {
			slog.Error("Dynamic log manager failed", "error", err)
			errChan <- err
		}
	}()

	// Start the job-status webhook dispatcher, if enabled.
	if dispatcher != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("Starting status webhook dispatcher")

			if err := dispatcher.Run(ctx); err != nil {
				slog.Error("Status webhook dispatcher failed", "error", err)
				errChan <- err
			}
		}()
	}

	// Start API server
	router := setupAPIRoute(logAggregator)
	server := &http.Server{
		Addr:              ":" + cfg.APIPort,
		Handler:           router,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("Starting API server", "port", cfg.APIPort)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("API server failed", "error", err)
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	return waitForShutdown(ctx, cancel, server, &wg, errChan)
}

// waitForShutdown waits for OS signal or error and performs graceful shutdown
func waitForShutdown(ctx context.Context, cancel context.CancelFunc, server *http.Server, wg *sync.WaitGroup, errChan chan error,
) error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var shutdownErr error

	select {
	case sig := <-sigChan:
		slog.Info("Received shutdown signal", "signal", sig.String())
	case err := <-errChan:
		slog.Error("Error during operation", "error", err)
		shutdownErr = err
	}

	// Cancel context to stop background goroutines
	slog.Info("Starting graceful shutdown...")
	cancel()

	// Shutdown API server with timeout. Derive from Background, not ctx: ctx was
	// just cancelled above, so basing the timeout on it would make Shutdown return
	// immediately instead of draining in-flight requests.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("API server shutdown error", "error", err)
		if shutdownErr == nil {
			shutdownErr = err
		}
	} else {
		slog.Info("API server shutdown complete")
	}

	// Wait for all goroutines to finish
	wg.Wait()
	slog.Info("Graceful shutdown complete")

	return shutdownErr
}

// setupAPIRoute creates and configures the Gin router with all log manager endpoints.
// It registers the following routes:
//
//   - GET /jobs/:jobId/logs   — returns all aggregated log entries for the given job ID. (Used for testing purposes)
//   - GET /jobs/:jobId/status — returns the current build status for the given job ID, or 404 if the job is not found.
//   - GET /jobs               — returns a list of all known job IDs (active and completed).
//   - GET /health             — liveness probe returning a static OK response.
func setupAPIRoute(aggregator buildlogs.LogAggregator) *gin.Engine {
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	jobs := r.Group("/jobs")
	{
		jobs.GET("/:jobId/logs", getJobLogs(aggregator))
		jobs.GET("/:jobId/status", getJobStatus(aggregator))
		jobs.GET("", listJobs(aggregator))
	}

	r.GET("/health", healthCheck())

	// Swagger UI (and its doc.json) are only exposed when DEBUG is enabled.
	if os.Getenv("DEBUG") == "true" {
		r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	return r
}

// getJobLogs returns the aggregated log entries for a job.
//
//	@Summary		Get job logs
//	@Description	Returns all aggregated log entries for the given job ID.
//	@Tags			jobs
//	@Produce		json
//	@Param			jobId	path		string	true	"Job ID"
//	@Success		200		{object}	map[string]interface{}
//	@Router			/jobs/{jobId}/logs [get]
func getJobLogs(aggregator buildlogs.LogAggregator) gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("jobId")
		logs := aggregator.GetJobLogs(jobID)
		c.JSON(200, gin.H{"logs": logs})
	}
}

// getJobStatus returns the current build status for a job.
//
//	@Summary		Get job status
//	@Description	Returns the current build status for the given job ID, or 404 if the job is unknown.
//	@Tags			jobs
//	@Produce		json
//	@Param			jobId	path		string				true	"Job ID"
//	@Success		200		{object}	map[string]string	"status"
//	@Failure		404		{object}	map[string]string	"error"
//	@Router			/jobs/{jobId}/status [get]
func getJobStatus(aggregator buildlogs.LogAggregator) gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("jobId")
		status, err := aggregator.GetJobStatus(jobID)
		if err != nil {
			c.JSON(404, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"status": status.String()})
	}
}

// listJobs returns all known job IDs (active and completed).
//
//	@Summary		List jobs
//	@Description	Returns a list of all known job IDs (active and completed).
//	@Tags			jobs
//	@Produce		json
//	@Success		200	{object}	map[string]interface{}
//	@Router			/jobs [get]
func listJobs(aggregator buildlogs.LogAggregator) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(200, gin.H{"jobs": aggregator.GetAllJobs()})
	}
}

// healthCheck is the liveness handler.
//
//	@Summary		Health check
//	@Description	Liveness probe returning a static OK response.
//	@Tags			health
//	@Produce		json
//	@Success		200	{object}	map[string]string
//	@Router			/health [get]
func healthCheck() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	}
}
