package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/nats-io/nats.go/jetstream"
)

// maxJobs caps the number of jobs returned by the list endpoint.
const maxJobs = 500

// StepView is the redacted, display-safe representation of a job step.
type StepView struct {
	ID              int               `json:"id"`
	Name            string            `json:"name"`
	Image           string            `json:"image"`
	Script          string            `json:"script"`
	ContinueOnError bool              `json:"continueOnError"`
	Metadata        map[string]string `json:"metadata"`
	CPULimit        uint              `json:"cpuLimit"`
	MemoryLimit     string            `json:"memoryLimit"`
	Network         string            `json:"network,omitempty"`
	MemorySwap      string            `json:"memorySwap,omitempty"`
	PidsLimit       int64             `json:"pidsLimit,omitempty"`
}

// JobDetail is the full job view: identity/status from the tracker plus the
// redacted payload from the KV store.
type JobDetail struct {
	JobSummary
	Timestamp      time.Time         `json:"timestamp"`
	Metadata       map[string]string `json:"metadata"`
	Steps          []StepView        `json:"steps"`
	TimeoutSeconds int64             `json:"timeoutSeconds,omitempty"`
	// PayloadAvailable is false when the full payload has aged out of the KV
	// store; identity/status from the tracker are still returned.
	PayloadAvailable bool `json:"payloadAvailable"`
}

// RegisterRoutes wires the dashboard's API routes and the SPA fallback onto r.
// When the dashboard is not configured, every /api route returns 503 so the
// feature fails closed rather than exposing data.
func (s *Server) RegisterRoutes(r *gin.Engine) {
	api := r.Group("/api")

	if !s.cfg.Enabled() {
		// Register a catch-all so any /api request fails closed with a clear 503
		// (group middleware alone would not run for otherwise-unmatched routes).
		api.Any("/*path", func(c *gin.Context) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "dashboard is not configured",
			})
		})
		return
	}

	api.POST("/login", s.handleLogin)

	authed := api.Group("")
	authed.Use(s.authMiddleware())
	{
		authed.POST("/logout", s.handleLogout)
		authed.GET("/session", s.handleSession)
		authed.GET("/jobs", s.handleListJobs)
		authed.GET("/jobs/:id", s.handleJobDetail)
		authed.GET("/jobs/:id/logs", s.handleJobLogs)
		authed.GET("/jobs/:id/logs/stream", s.handleJobLogStream)
		authed.GET("/metrics", s.handleMetrics)
		authed.GET("/stream", s.handleStream)
	}
}

func (s *Server) handleMetrics(c *gin.Context) {
	c.JSON(http.StatusOK, s.metrics(c.Request.Context()))
}

// handleListJobs returns tracked jobs, enriched with name/step count from the KV
// payload, most-recently-updated first.
func (s *Server) handleListJobs(c *gin.Context) {
	statusFilter := c.Query("status")
	jobs := s.tracker.list(statusFilter)

	// Sort newest first by the best timestamp available.
	sort.Slice(jobs, func(i, j int) bool {
		return jobTime(jobs[i]).After(jobTime(jobs[j]))
	})
	if len(jobs) > maxJobs {
		jobs = jobs[:maxJobs]
	}

	// The tracker already carries name and step count for jobs this replica
	// enqueued, so a KV read is only needed for the rare job we learned about
	// purely from a status event (no name). The result, including a definitive
	// miss, is written back so each such job costs one KV read in total rather
	// than one per list request.
	for i := range jobs {
		if jobs[i].Name != "" || !s.tracker.needsIdentity(jobs[i].ID) {
			continue
		}
		ident, ok := s.fetchIdentity(c.Request.Context(), jobs[i].ID)
		if !ok {
			continue
		}
		s.tracker.setIdentity(jobs[i].ID, ident.Name, len(ident.Steps))
		jobs[i].Name = ident.Name
		if jobs[i].StepCount == 0 {
			jobs[i].StepCount = len(ident.Steps)
		}
	}

	c.JSON(http.StatusOK, gin.H{"jobs": jobs})
}

// handleJobDetail returns the redacted full definition of a job.
func (s *Server) handleJobDetail(c *gin.Context) {
	id := c.Param("id")

	detail := JobDetail{Metadata: map[string]string{}, Steps: []StepView{}}
	if summary, ok := s.tracker.get(id); ok {
		detail.JobSummary = summary
	} else {
		detail.JobSummary = JobSummary{ID: id, Status: "Unknown"}
	}

	job, ok := s.fetchPayload(c.Request.Context(), id)
	if !ok {
		c.JSON(http.StatusOK, detail)
		return
	}
	detail.PayloadAvailable = true
	detail.Timestamp = job.Timestamp
	detail.TimeoutSeconds = job.TimeoutSeconds
	if detail.Name == "" {
		detail.Name = job.Name
	}
	detail.StepCount = len(job.Steps)

	// Redact before anything leaves the process.
	redacted := s.redactor.Payload(job)
	detail.Metadata = nonNil(redacted.Metadata)
	detail.Steps = make([]StepView, len(redacted.Steps))
	for i, st := range redacted.Steps {
		detail.Steps[i] = StepView{
			ID:              st.ID,
			Name:            st.Name,
			Image:           st.Image,
			Script:          st.Script,
			ContinueOnError: st.ContinueOnError,
			Metadata:        nonNil(st.Metadata),
			CPULimit:        st.CPULimit,
			MemoryLimit:     st.MemoryLimit,
			Network:         st.Network,
			MemorySwap:      st.MemorySwap,
			PidsLimit:       st.PidsLimit,
		}
	}

	c.JSON(http.StatusOK, detail)
}

// handleJobLogs proxies to the internal HadesLogManager, which aggregates logs.
// Auth is already enforced by the route group, so the unauthenticated internal
// service is never exposed directly.
func (s *Server) handleJobLogs(c *gin.Context) {
	// The id is forwarded into an upstream URL path. Parse it as a UUID and use
	// the canonical re-serialized form so only a well-formed job id can ever
	// reach the log manager - no user-controlled data flows into the request URL.
	parsedID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job id"})
		return
	}
	target, err := url.JoinPath(s.cfg.LogManagerURL, "jobs", parsedID.String(), "logs")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid log manager URL"})
		return
	}

	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, target, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build log request"})
		return
	}
	resp, err := s.http.Do(req)
	if err != nil {
		slog.Warn("Log manager unreachable", "job_id", parsedID, "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "log service unavailable"})
		return
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to read logs"})
		return
	}
	c.Data(resp.StatusCode, "application/json", body)
}

// fetchPayload reads and unmarshals a job's full payload from the KV bucket.
func (s *Server) fetchPayload(ctx context.Context, id string) (payload.QueuePayload, bool) {
	var job payload.QueuePayload
	if found, _ := s.getStoredJob(ctx, id, &job); !found {
		return payload.QueuePayload{}, false
	}
	return job, true
}

// jobIdentity is the part of a stored payload the job list shows. Steps are
// left undecoded since only their count is needed.
type jobIdentity struct {
	Name  string            `json:"name"`
	Steps []json.RawMessage `json:"steps"`
}

// fetchIdentity reads a job's name and steps from the KV bucket. ok is false
// only for a transient failure worth retrying on a later request; a missing or
// undecodable payload is reported as ok with a zero identity so it is cached.
func (s *Server) fetchIdentity(ctx context.Context, id string) (ident jobIdentity, ok bool) {
	found, transient := s.getStoredJob(ctx, id, &ident)
	if !found {
		return jobIdentity{}, !transient
	}
	return ident, true
}

// getStoredJob unmarshals the job's stored payload into out. found is false
// when there is nothing usable; transient then says whether a later read might
// succeed (KV unavailable or erroring) as opposed to a missing or undecodable
// entry. out may be partially written when found is false.
func (s *Server) getStoredJob(ctx context.Context, id string, out any) (found, transient bool) {
	if s.kv == nil {
		return false, true
	}
	entry, err := s.kv.Get(ctx, id)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return false, false
		}
		slog.Debug("KV get failed", "job_id", id, "error", err)
		return false, true
	}
	if err := json.Unmarshal(entry.Value(), out); err != nil {
		slog.Warn("Failed to unmarshal job payload", "job_id", id, "error", err)
		return false, false
	}
	return true, false
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// jobTime returns the most relevant timestamp for ordering a job newest-first.
func jobTime(j JobSummary) time.Time {
	switch {
	case j.FinishedAt != nil:
		return *j.FinishedAt
	case j.StartedAt != nil:
		return *j.StartedAt
	case j.QueuedAt != nil:
		return *j.QueuedAt
	default:
		return time.Time{}
	}
}
