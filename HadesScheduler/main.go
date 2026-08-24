package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hades-scheduler/hades/hadesScheduler/docker"
	"github.com/hades-scheduler/hades/hadesScheduler/k8s"
	"github.com/hades-scheduler/hades/hadesScheduler/log"
	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/metrics"
	hadesnats "github.com/hades-scheduler/hades/shared/nats"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/hades-scheduler/hades/shared/timing"
	"github.com/hades-scheduler/hades/shared/utils"
	"github.com/prometheus/client_golang/prometheus"
)

// HadesSchedulerConfig holds the scheduler's runtime configuration. The
// executor is selected separately via utils.ExecutorConfig (HADES_EXECUTOR).
type HadesSchedulerConfig struct {
	// Concurrency moved into ConsumerConfig alongside AckWait and MaxDeliver:
	// the three are one contract (how many workers, how long a job may hold a
	// message, how many deliveries it gets) and splitting them across two
	// structs made it possible to set one without the others.
	ConsumerConfig hadesnats.ConsumerConfig
	MetricsPort    uint `env:"METRICS_PORT,notEmpty" envDefault:"8082"`
	NatsConfig     hadesnats.ConnectionConfig
}

func main() {
	utils.SetupLogging()

	var cfg HadesSchedulerConfig
	if err := utils.LoadConfig(&cfg); err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	var executorCfg utils.ExecutorConfig
	if err := utils.LoadConfig(&executorCfg); err != nil {
		slog.Error("Failed to load executor configuration", "error", err)
		os.Exit(1)
	}

	slog.Info("HadesScheduler configuration",
		"executor", executorCfg.Executor,
		"concurrency", cfg.ConsumerConfig.Concurrency,
		"metrics_port", cfg.MetricsPort,
		"nats_url", cfg.NatsConfig.URL,
		"nats_tls", cfg.NatsConfig.TLS,
		"nats_ack_wait", cfg.ConsumerConfig.AckWait,
		"nats_max_deliver", cfg.ConsumerConfig.MaxDeliver,
	)

	natsConn, err := hadesnats.SetupDefaultNatsConnection(cfg.NatsConfig)
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer natsConn.Close()

	consumer, err := hadesnats.NewHadesConsumer(natsConn, cfg.ConsumerConfig)
	if err != nil {
		slog.Error("Failed to create Hades consumer", "error", err)
		os.Exit(1)
	}

	var scheduler hades.JobScheduler
	switch executorCfg.Executor {
	case "k8s":
		slog.Info("Started HadesScheduler in Kubernetes mode")
		k8sScheduler, err := k8s.NewK8sScheduler(natsConn)
		if err != nil {
			slog.Error("Failed to create k8s scheduler", "error", err)
			os.Exit(1)
		}
		scheduler = k8sScheduler

	case "docker":
		slog.Info("Started HadesScheduler in Docker mode")

		var dockerCfg docker.EnvConfig
		if err := utils.LoadConfig(&dockerCfg); err != nil {
			slog.Error("Failed to load Docker executor configuration", "error", err)
			os.Exit(1)
		}
		slog.Debug("Docker config", "config", dockerCfg)

		publisher, err := log.NewNATSPublisher(natsConn)
		if err != nil {
			slog.Error("Failed to create NATS publisher", "error", err)
			os.Exit(1)
		}

		scheduler, err = docker.NewScheduler(
			docker.WithDockerHost(dockerCfg.DockerHost),
			docker.WithScriptExecutor(dockerCfg.DockerScriptExecutor),
			docker.WithContainerAutoremove(dockerCfg.ContainerAutoremove),
			docker.WithCPULimit(dockerCfg.CPULimit),
			docker.WithMemoryLimit(dockerCfg.MemoryLimit),
			docker.WithLogPublisher(publisher),
			docker.WithStatusPublisher(publisher),
		)
		if err != nil {
			slog.Error("Failed to create Docker scheduler", "error", err)
			os.Exit(1)
		}
	default:
		slog.Error("Invalid executor specified: ", "executor", executorCfg.Executor)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("Received shutdown signal", "signal", sig.String())
		cancel()
	}()

	// Expose the phase-timing histograms on the default registry that
	// metrics.Serve scrapes, and enable tracing (noop unless an OTLP endpoint is
	// configured).
	timing.MustRegister(prometheus.DefaultRegisterer)
	tracingShutdown, err := timing.InitTracing(ctx, "hades-scheduler")
	if err != nil {
		slog.Error("Failed to init tracing", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tracingShutdown(shutdownCtx)
	}()

	// Prometheus metrics on a dedicated, cluster-internal port. This is the
	// scheduler's only HTTP listener; it stops when the context is cancelled.
	go func() {
		// Metrics are auxiliary: log and keep scheduling jobs rather than taking
		// the scheduler down (and skipping the deferred tracing/NATS shutdown that
		// os.Exit from a goroutine would bypass).
		if err := metrics.Serve(ctx, fmt.Sprintf(":%d", cfg.MetricsPort)); err != nil {
			slog.Error("Metrics server failed; continuing without metrics", "error", err)
		}
	}()

	consumer.DequeueJob(ctx, func(p payload.QueuePayload) {
		slog.Info("Received job", "id", p.ID.String())
		slog.Debug("Job payload", "payload", p)

		if err := scheduler.ScheduleJob(ctx, p); err != nil {
			slog.Error("Failed to schedule job", "error", err, "id", p.ID.String())
			jobsScheduledTotal.WithLabelValues("error").Inc()
			return
		}
		jobsScheduledTotal.WithLabelValues("success").Inc()
		slog.Info("Successfully scheduled job", "id", p.ID.String())
	})

	slog.Info("Scheduler shutdown complete")
}
