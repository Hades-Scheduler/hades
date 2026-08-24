/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/hades-scheduler/hades/hadesScheduler/log"
	"github.com/hades-scheduler/hades/shared/timing"
	"github.com/hades-scheduler/hades/shared/utils"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	hadesnats "github.com/hades-scheduler/hades/shared/nats"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	"github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/internal/controller"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

type NSConfig struct {
	WatchNamespace string `env:"WATCH_NAMESPACE"`
}

type OperatorConfig struct {
	DeleteOnComplete bool `env:"DELETE_ON_COMPLETE" envDefault:"true"`
	MaxParallelism   uint `env:"MAX_PARALLELISM" envDefault:"100"`
}

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(buildv1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

const DefaultMaxParallelism = 100

// nolint:gocyclo
func main() {
	var enableLeaderElection bool
	var probeAddr string
	var metricsAddr string
	var enableDevMode bool

	if os.Getenv("DEV_MODE") == "true" {
		enableDevMode = true
	}

	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8083", "The address the probe endpoint binds to.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8082", "The address the metrics endpoint binds to. Set to \"0\" to disable.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	opts := zap.Options{Development: enableDevMode}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	var nsConfig NSConfig
	if err := utils.LoadConfig(&nsConfig); err != nil {
		setupLog.Error(err, "unable to load namespace configuration")
		os.Exit(1)
	}

	var operatorConfig OperatorConfig
	if err := utils.LoadConfig(&operatorConfig); err != nil {
		setupLog.Error(err, "unable to load operator configuration")
		os.Exit(1)
	}

	if operatorConfig.MaxParallelism == 0 {
		setupLog.WithValues("env", "MAX_PARALLELISM").
			Info("MAX_PARALLELISM is 0 or invalid; falling back to default", "fallback", DefaultMaxParallelism)
		operatorConfig.MaxParallelism = DefaultMaxParallelism
	}

	if nsConfig.WatchNamespace != "" {
		setupLog.Info("scoping cache to a single namespace", "namespace", nsConfig.WatchNamespace)
	} else {
		setupLog.Info("no WATCH_NAMESPACE set; manager will watch cluster-wide")
	}

	mgrOpts := ctrl.Options{
		Scheme: scheme,
		// controller-runtime serves its own registry here (reconcile/workqueue/Go
		// runtime metrics) over plain HTTP on a dedicated, cluster-internal port.
		// SecureServing stays off for parity with the other Hades services; the
		// port is never exposed via the public ingress. Set to "0" to disable.
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "715d8f3b.hades.tum.de",
	}

	if nsConfig.WatchNamespace != "" {
		mgrOpts.Cache = cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				nsConfig.WatchNamespace: {},
			},
		}
	}

	// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
	// when the Manager ends. This requires the binary to immediately end when the
	// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
	// speeds up voluntary leader transitions as the new leader don't have to wait
	// LeaseDuration time first.
	//
	// In the default scaffold provided, the program ends immediately after
	// the manager stops, so would be fine to enable this option. However,
	// if you are doing or is intended to do any operation such as perform cleanups
	// after the manager stops then its usage might be unsafe.
	// LeaderElectionReleaseOnCancel: true,
	cfg := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(cfg, mgrOpts)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Serve the Hades timing histograms on controller-runtime's registry, i.e. the
	// same /metrics endpoint (metricsAddr) as its built-in reconcile/workqueue
	// metrics.
	timing.MustRegister(ctrlmetrics.Registry)

	// Enable OpenTelemetry tracing when OTEL_EXPORTER_OTLP_ENDPOINT is set; noop
	// otherwise. The operator's backdated step spans nest under the job trace
	// propagated via the BuildJob annotation.
	tracingShutdown, err := timing.InitTracing(context.Background(), "hades-operator")
	if err != nil {
		setupLog.Error(err, "unable to init tracing")
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tracingShutdown(shutdownCtx)
	}()

	var natsConfig hadesnats.ConnectionConfig
	if err := utils.LoadConfig(&natsConfig); err != nil {
		setupLog.Error(err, "unable to load NATS configuration")
		os.Exit(1)
	}

	setupLog.Info("HadesOperator configuration",
		"watch_namespace", nsConfig.WatchNamespace,
		"cluster_wide", nsConfig.WatchNamespace == "",
		"delete_on_complete", operatorConfig.DeleteOnComplete,
		"max_parallelism", operatorConfig.MaxParallelism,
		"metrics_addr", metricsAddr,
		"dev_mode", enableDevMode,
		"nats_url", natsConfig.URL,
		"nats_tls", natsConfig.TLS,
	)

	nc, err := hadesnats.SetupDefaultNatsConnection(natsConfig)
	if err != nil {
		setupLog.Error(err, "unable to setup NATS Connection")
		os.Exit(1)
	}

	publisher, err := log.NewNATSPublisher(nc)
	if err != nil {
		setupLog.Error(err, "unable to create NATS publisher")
		os.Exit(1)
	}

	kcs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		setupLog.Error(err, "unable to init kubernetes clientset")
		os.Exit(1)
	}

	if err := (&controller.BuildJobReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		K8sClient:        kcs,
		DeleteOnComplete: operatorConfig.DeleteOnComplete,
		MaxParallelism:   operatorConfig.MaxParallelism,
		Publisher:        publisher,
		LogStreams:       controller.NewLogStreamRegistry(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "BuildJob")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
