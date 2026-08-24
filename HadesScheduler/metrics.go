package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// jobsScheduledTotal counts jobs the scheduler dispatched to the executor, by
// outcome: "success" or "error".
var jobsScheduledTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "hades",
	Name:      "jobs_scheduled_total",
	Help:      "Total number of jobs scheduled by the executor, labelled by result.",
}, []string{"result"})
