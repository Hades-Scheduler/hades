package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakePublisher is a no-op JobPublisher for testing the metrics wiring without
// a real NATS connection.
type fakePublisher struct{}

func (fakePublisher) EnqueueJobWithPriority(_ context.Context, _ payload.QueuePayload, _ hades.Priority) error {
	return nil
}

func TestBuildRequestIncrementsMetrics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := setupRouter("", fakePublisher{}, nil, nil)

	before := testutil.ToFloat64(buildRequestsTotal.WithLabelValues("accepted"))

	restPayload := payload.RESTPayload{
		Priority: 1,
		QueuePayload: payload.QueuePayload{
			Name:      "example",
			Timestamp: time.Now(),
			Steps: []payload.Step{
				{ID: 1, Name: "step1", Image: "image1", Script: "script1"},
			},
		},
	}
	body, _ := json.Marshal(restPayload)
	req, _ := http.NewRequest(http.MethodPost, "/build", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("POST /build status = %d, want 200 (%s)", w.Code, w.Body.String())
	}

	after := testutil.ToFloat64(buildRequestsTotal.WithLabelValues("accepted"))
	if after != before+1 {
		t.Fatalf("hades_build_requests_total{accepted} = %v, want %v", after, before+1)
	}
}
