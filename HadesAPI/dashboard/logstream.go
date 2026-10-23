package dashboard

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/nats-io/nats.go/jetstream"
)

// logStreamBuffer bounds how many log batches are buffered between the JetStream
// consumer and the SSE writer before backpressure throttles the consumer.
const logStreamBuffer = 64

// handleJobLogStream streams a job's build logs to the client live over SSE.
//
// It opens a per-connection ephemeral (ordered) JetStream consumer on
// hades.logs.<jobID> with DeliverAllPolicy, so a newly connected client receives
// the full retained backlog followed by the live tail from a single subscription
// - independently of the HadesLogManager. Because a live job produces logs
// continuously, the connection is long-lived: the shared http.Server runs with
// WriteTimeout disabled and this handler additionally clears the per-connection
// write deadline.
//
// All writes to the response happen in this one goroutine; the JetStream callback
// only hands batches to it over a channel, so gin.ResponseWriter is never written
// concurrently. The consumer is torn down when the client disconnects.
func (s *Server) handleJobLogStream(c *gin.Context) {
	// Parse the id as a UUID so only a well-formed job id reaches the subject
	// filter - no user-controlled data flows into NATS.
	parsedID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job id"})
		return
	}
	subject := fmt.Sprintf(buildlogs.NatsLogSubject, parsedID.String())

	if s.js == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "log service unavailable"})
		return
	}

	w := c.Writer
	// Best-effort: remove any write deadline so the stream can stay open.
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.SetWriteDeadline(time.Time{})
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	ctx := c.Request.Context()

	// An ordered consumer is ephemeral and per-connection, so two viewers of the
	// same job never share (and steal from) one consumer; it is cleaned up when
	// Consume stops or the client goes away.
	cons, err := s.js.OrderedConsumer(ctx, buildlogs.StreamName, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		slog.Warn("Failed to create log stream consumer", "job_id", parsedID, "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "log service unavailable"})
		return
	}

	logs := make(chan json.RawMessage, logStreamBuffer)
	consumeCtx, err := cons.Consume(func(msg jetstream.Msg) {
		// The stored message is already a JSON-encoded buildlogs.Log, the exact
		// shape the SPA expects, so it is forwarded without a decode/encode pass.
		// Marshaling the event compacts it, so a stray newline cannot split the
		// SSE frame; invalid JSON is dropped here because Marshal would reject
		// the whole event.
		if !json.Valid(msg.Data()) {
			slog.Debug("Skipping malformed streamed log", "job_id", parsedID)
			return
		}
		l := json.RawMessage(msg.Data())
		// Block only until the client catches up or disconnects; a full buffer
		// throttles the consumer rather than dropping logs.
		select {
		case logs <- l:
		case <-ctx.Done():
		}
	})
	if err != nil {
		slog.Warn("Failed to start log stream consumer", "job_id", parsedID, "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "log service unavailable"})
		return
	}
	defer consumeCtx.Stop()

	// Flush the (already-written) 200 response now so EventSource reaches its OPEN
	// state immediately, instead of only once the first log batch arrives - a job
	// may be idle for a while before it emits anything.
	w.Flush()

	// Heartbeat keeps intermediaries from closing an idle connection.
	heartbeat := time.NewTicker(25 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case l := <-logs:
			writeEvent(w, event{Type: eventLog, Log: l})
			w.Flush()
		case <-heartbeat.C:
			if _, err := w.WriteString(": ping\n\n"); err != nil {
				return
			}
			w.Flush()
		}
	}
}
