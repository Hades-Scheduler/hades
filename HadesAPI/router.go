package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/hades-scheduler/hades/hadesAPI/dashboard"
	_ "github.com/hades-scheduler/hades/hadesAPI/docs" // generated OpenAPI spec (make docs-api)
	"github.com/hades-scheduler/hades/hadesAPI/web"
	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/hades-scheduler/hades/shared/timing"
	"github.com/hades-scheduler/hades/shared/utils"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// contentSecurityPolicy is scoped to what the embedded SPA needs: same-origin
// scripts (hashed asset bundles, no inline JS), same-origin plus inline styles
// (Tailwind + Recharts inject style attributes), data: images (the favicon),
// and same-origin fetch/EventSource. Framing is denied.
const contentSecurityPolicy = "default-src 'self'; " +
	"script-src 'self'; " +
	"style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data:; " +
	"font-src 'self'; " +
	"connect-src 'self'; " +
	"object-src 'none'; " +
	"base-uri 'self'; " +
	"form-action 'self'; " +
	"frame-ancestors 'none'"

// securityHeaders sets defensive response headers on every route. The strict CSP
// is skipped for the DEBUG-only Swagger UI, which relies on inline scripts/styles.
func securityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.Writer.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		// Ignored over plain HTTP; activates once served via TLS.
		h.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		if !strings.HasPrefix(c.Request.URL.Path, "/swagger") {
			h.Set("Content-Security-Policy", contentSecurityPolicy)
		}
		c.Next()
	}
}

func setupRouter(authKey string, producer hades.JobPublisher, statusPublisher buildstatus.StatusPublisher, dash *dashboard.Server) *gin.Engine {
	r := gin.New()
	r.Use(gin.ErrorLogger())
	r.Use(gin.Recovery())
	r.Use(securityHeaders())

	// Report validation errors using the JSON field names (e.g. "name")
	// rather than the Go struct field names (e.g. "Name").
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		v.RegisterTagNameFunc(func(fld reflect.StructField) string {
			name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
			if name == "-" {
				return ""
			}
			return name
		})
	}

	r.GET("/ping", ping)

	// Swagger UI (and its doc.json) are only exposed when DEBUG is enabled, so
	// the API contract is never served in production.
	if os.Getenv("DEBUG") == "true" {
		r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	}

	api := r.Group("/")
	if authKey == "" {
		slog.Warn("No auth key set")
	} else {
		slog.Info("Auth key set")
		api.Use(gin.BasicAuth(gin.Accounts{"hades": authKey}))
	}

	api.POST("/build", func(c *gin.Context) {
		addBuildToQueue(c, producer, statusPublisher, dash)
	})

	// Mount the operator dashboard (API + embedded SPA). When configured it
	// registers /api routes and the SPA fallback; when unconfigured its /api
	// routes return 503 and the SPA is not served.
	if dash != nil {
		dash.RegisterRoutes(r)
		if dash.Enabled() {
			web.Register(r)
		}
	}

	return r
}

// ping is the liveness handler.
//
//	@Summary		Health check
//	@Description	Liveness probe returning a status string and the current server time.
//	@Tags			health
//	@Produce		json
//	@Success		200	{object}	map[string]string
//	@Router			/ping [get]
func ping(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "ok",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// bindErrorMessage turns a request-binding error into a human-readable message
// that names the offending field(s). Validation failures (e.g. a missing
// required field) are reported per field; any other error is treated as a
// malformed request body.
func bindErrorMessage(err error) string {
	var verrs validator.ValidationErrors
	if errors.As(err, &verrs) {
		parts := make([]string, 0, len(verrs))
		for _, fe := range verrs {
			switch fe.Tag() {
			case "required":
				parts = append(parts, fmt.Sprintf("%q is required", fe.Field()))
			default:
				parts = append(parts, fmt.Sprintf("%q failed %q validation", fe.Field(), fe.Tag()))
			}
		}
		return "Invalid request payload: " + strings.Join(parts, "; ")
	}
	return "Invalid JSON body"
}

// addBuildToQueue validates a job payload and enqueues it on NATS by priority.
//
//	@Summary		Enqueue a build job
//	@Description	Validates a multi-step job, assigns a UUID, and publishes it to NATS on the queue matching its priority. Requires HTTP Basic Auth when the server is started with an AUTH_KEY.
//	@Tags			jobs
//	@Accept			json
//	@Produce		json
//	@Param			payload	body		payload.RESTPayload	true	"Job definition"
//	@Success		200		{object}	map[string]string	"job_id of the enqueued job"
//	@Failure		400		{string}	string				"Invalid request payload"
//	@Failure		500		{string}	string				"Failed to enqueue job"
//	@Security		BasicAuth
//	@Router			/build [post]
func addBuildToQueue(c *gin.Context, producer hades.JobPublisher, statusPublisher buildstatus.StatusPublisher, dash *dashboard.Server) {
	var p payload.RESTPayload
	p.Priority = 3
	if err := c.ShouldBind(&p); err != nil {
		msg := bindErrorMessage(err)
		slog.Error("Failed to bind request payload", "error", err)
		buildRequestsTotal.WithLabelValues("rejected").Inc()
		c.String(http.StatusBadRequest, msg)
		return
	}

	if p.QueuePayload.TimeoutSeconds < 0 {
		slog.Error("Invalid timeout_seconds", "value", p.QueuePayload.TimeoutSeconds)
		c.String(http.StatusBadRequest, "timeout_seconds cannot be negative")
		return
	}
	if p.QueuePayload.TimeoutSeconds > payload.MaxTimeoutSeconds {
		slog.Error("timeout_seconds too large", "value", p.QueuePayload.TimeoutSeconds)
		c.String(http.StatusBadRequest, fmt.Sprintf("timeout_seconds must not exceed %d", payload.MaxTimeoutSeconds))
		return
	}

	for _, step := range p.QueuePayload.Steps {
		var memLimitBytes int64
		if step.MemoryLimit != "" {
			parsed, err := utils.ParseMemoryLimit(step.MemoryLimit)
			if err != nil {
				slog.Error("Failed to parse RAM limit", "error", err)
				buildRequestsTotal.WithLabelValues("rejected").Inc()
				c.String(http.StatusBadRequest, "Failed to parse RAM limit")
				return
			}
			memLimitBytes = parsed
		}

		if step.MemorySwap != "" {
			swapBytes, err := utils.ParseMemoryLimit(step.MemorySwap)
			if err != nil {
				slog.Error("Failed to parse memory_swap limit", "error", err)
				c.String(http.StatusBadRequest, "Failed to parse memory_swap limit")
				return
			}
			// Docker requires memory_swap (total memory+swap) to be set together
			// with a memory limit and to be at least as large as it.
			if step.MemoryLimit == "" {
				c.String(http.StatusBadRequest, "memory_swap requires memory_limit to be set")
				return
			}
			if swapBytes < memLimitBytes {
				c.String(http.StatusBadRequest, "memory_swap must be greater than or equal to memory_limit")
				return
			}
		}

		if step.PidsLimit < 0 {
			c.String(http.StatusBadRequest, "pids_limit cannot be negative")
			return
		}

		if err := utils.ValidateNetworkMode(step.Network); err != nil {
			slog.Error("Invalid network mode", "error", err)
			c.String(http.StatusBadRequest, err.Error())
			return
		}
	}

	if p.QueuePayload.CallbackURL != "" {
		if err := utils.ValidateCallbackURL(p.QueuePayload.CallbackURL); err != nil {
			slog.Error("Invalid callback_url", "error", err)
			buildRequestsTotal.WithLabelValues("rejected").Inc()
			c.String(http.StatusBadRequest, "Invalid callback_url: "+err.Error())
			return
		}
	}

	if p.QueuePayload.StatusCallbackURL != "" {
		if err := utils.ValidateCallbackURL(p.QueuePayload.StatusCallbackURL); err != nil {
			slog.Error("Invalid status_callback_url", "error", err)
			c.String(http.StatusBadRequest, "Invalid status_callback_url: "+err.Error())
			return
		}
	}

	p.QueuePayload.ID = uuid.New()
	// Stamp the server-side submission time so queue_wait (submit -> scheduler
	// dequeue) is measured against a trusted clock rather than a client value.
	p.QueuePayload.Timestamp = time.Now()
	slog.Debug("Received build request ", "payload", SafePayloadFormat(p.QueuePayload))

	queuePrio := hades.PriorityFromInt(p.Priority)

	// Open the job's root span and propagate its context in the payload so the
	// scheduler/operator spans nest under it. Noop unless tracing is enabled.
	// Always assign server-side (never conditionally): TraceParent is bound from
	// the request body, so a client-supplied value must not survive into NATS and
	// the BuildJob annotation when tracing is disabled and Inject returns nil.
	spanCtx, endSpan := timing.StartSpan(c.Request.Context(), "hades.enqueue")
	if carrier := timing.Inject(spanCtx); carrier != nil {
		p.QueuePayload.TraceParent = carrier["traceparent"]
	} else {
		p.QueuePayload.TraceParent = ""
	}
	defer endSpan()

	err := producer.EnqueueJobWithPriority(spanCtx, p.QueuePayload, queuePrio)
	if err != nil {
		slog.Error("Failed to enqueue job", "error", err)
		buildRequestsTotal.WithLabelValues("rejected").Inc()
		c.String(http.StatusInternalServerError, "Failed to enqueue job")
		return
	}

	buildRequestsTotal.WithLabelValues("accepted").Inc()
	jobsEnqueuedTotal.WithLabelValues(string(queuePrio)).Inc()

	// Announce the job as Queued so lifecycle subscribers (HadesLogManager, the
	// dashboard live feed) see it immediately. This is best-effort: a publish
	// failure must not fail the enqueue that already succeeded.
	if statusPublisher != nil {
		if err := statusPublisher.PublishJobStatus(c.Request.Context(), buildstatus.StatusQueued, p.QueuePayload.ID.String()); err != nil {
			slog.Warn("Failed to publish Queued status", "job_id", p.QueuePayload.ID.String(), "error", err)
		}
	}

	// Record the job (with its priority, which the KV payload does not carry) so
	// the dashboard shows it immediately.
	if dash != nil {
		dash.TrackEnqueue(p.QueuePayload.ID.String(), p.QueuePayload.Name, len(p.QueuePayload.Steps), queuePrio)
	}

	slog.Info("Successfully enqueued job", "job_id", p.QueuePayload.ID.String())
	c.JSON(http.StatusOK, gin.H{
		"message": "Successfully enqueued job",
		"job_id":  p.QueuePayload.ID.String(),
	})
}
