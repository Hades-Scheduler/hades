package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// buildRequestsTotal counts /build requests by outcome: "accepted" once a job
// is enqueued, "rejected" when validation or enqueueing fails.
var buildRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "hades",
	Name:      "build_requests_total",
	Help:      "Total number of build requests received, labelled by result.",
}, []string{"result"})

// jobsEnqueuedTotal counts jobs successfully published to NATS, by priority.
var jobsEnqueuedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "hades",
	Name:      "jobs_enqueued_total",
	Help:      "Total number of jobs enqueued on NATS, labelled by priority.",
}, []string{"priority"})
