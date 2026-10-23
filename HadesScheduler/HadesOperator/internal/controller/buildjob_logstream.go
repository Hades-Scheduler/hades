package controller

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	buildv1 "github.com/hades-scheduler/hades/HadesScheduler/HadesOperator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// logStreamPersistThrottle bounds how often a container's streaming progress
// (LogsStreamedUntil) is written back to the BuildJob status. Persisting on every
// flush would trigger a reconcile/etcd write storm, so progress is tracked in
// memory and only persisted once this much wall-clock time has advanced.
const logStreamPersistThrottle = 30 * time.Second

// logStreamEventBuffer is the capacity of the registry's finished-stream event
// channel. Sends never block (see notifyFinished), so this only decides how many
// completions can pile up before the controller's channel source drains them.
const logStreamEventBuffer = 1024

// logStreamKey uniquely identifies a container's live log stream.
func logStreamKey(namespace, jobID, container string) string {
	return namespace + "/" + jobID + "/" + container
}

// splitLogStreamKey reverses logStreamKey. Namespaces, object names and container
// names are DNS labels/subdomains, so none of them can contain a "/".
func splitLogStreamKey(key string) (namespace, jobID string, ok bool) {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) != 3 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// logStreamJobPrefix is the key prefix shared by all of a BuildJob's container
// streams. It includes the namespace so same-named BuildJobs in different
// namespaces (possible in cluster-wide mode) never match each other.
func logStreamJobPrefix(namespace, jobID string) string {
	return namespace + "/" + jobID + "/"
}

// logStream is a single running (or just-finished) container log-follow goroutine.
type logStream struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu         sync.Mutex
	lastTS     time.Time // in-memory streaming progress
	err        error     // set before done is closed
	finishedAt time.Time // set before done is closed
}

func (s *logStream) recordProgress(t time.Time) {
	s.mu.Lock()
	if t.After(s.lastTS) {
		s.lastTS = t
	}
	s.mu.Unlock()
}

func (s *logStream) progress() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastTS
}

// result reports whether the stream goroutine has returned and, if so, its error.
func (s *logStream) result() (finished bool, err error) {
	finished, _, err = s.resultAt()
	return finished, err
}

// resultAt is result plus the time the stream goroutine returned.
func (s *logStream) resultAt() (finished bool, at time.Time, err error) {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return true, s.finishedAt, s.err
	default:
		return false, time.Time{}, nil
	}
}

// logStreamRegistry tracks the live log-follow goroutines the operator runs, one
// per container, so they can be started idempotently and cancelled when a job
// completes or is deleted. Goroutines are rooted at context.Background so they
// outlive the per-reconcile context; their lifetime is bounded by cancel.
type logStreamRegistry struct {
	mu      sync.Mutex
	streams map[string]*logStream

	// events carries one GenericEvent per cleanly finished stream so the
	// controller reconciles its BuildJob right away. Without it, a terminated
	// container (most often the finalizer, which exits within a second) would only
	// be marked published on the next timed requeue, delaying every completion.
	events chan event.GenericEvent
}

// NewLogStreamRegistry creates an empty registry.
func NewLogStreamRegistry() *logStreamRegistry {
	return &logStreamRegistry{
		streams: make(map[string]*logStream),
		events:  make(chan event.GenericEvent, logStreamEventBuffer),
	}
}

// Events is the channel the controller watches (via source.Channel) to learn
// that a BuildJob's log stream has finished.
func (r *logStreamRegistry) Events() <-chan event.GenericEvent {
	return r.events
}

// notifyFinished enqueues the BuildJob owning key. It never blocks: the stream
// goroutine must exit even if the controller is not consuming events (not
// started yet, or in unit tests), and a dropped event only costs latency because
// the drain and resync requeues still pick the job up.
func (r *logStreamRegistry) notifyFinished(key string) {
	if r.events == nil {
		return
	}
	namespace, jobID, ok := splitLogStreamKey(key)
	if !ok {
		return
	}
	ev := event.GenericEvent{Object: &buildv1.BuildJob{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: jobID}}}
	select {
	case r.events <- ev:
	default:
		slog.Debug("Log stream event channel full; relying on requeue", "key", key)
	}
}

// ensureStarted launches a streaming goroutine for key if one is not already
// running. It is idempotent: subsequent calls for the same key are no-ops while
// the stream exists. run receives a cancellable context and a progress recorder.
// It returns true only when it actually started a new stream, so the caller can
// perform one-time, ordered side effects (e.g. registering the container's slot).
func (r *logStreamRegistry) ensureStarted(key string, run func(ctx context.Context, progress func(time.Time)) error) (started bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.streams == nil {
		r.streams = make(map[string]*logStream)
	}
	if _, ok := r.streams[key]; ok {
		return false
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &logStream{cancel: cancel, done: make(chan struct{})}
	r.streams[key] = s

	go func() {
		err := run(ctx, s.recordProgress)
		s.mu.Lock()
		s.err = err
		s.finishedAt = time.Now()
		s.mu.Unlock()
		close(s.done)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Container log stream ended with error", "key", key, "error", err)
		}
		// Only a clean end is announced. A failed stream is restarted by the next
		// reconcile, and waking that reconcile immediately would turn a stream that
		// keeps failing (API server trouble, missing RBAC) into a hot loop; the
		// timed requeues pace those retries instead.
		if err == nil {
			r.notifyFinished(key)
		}
	}()
	return true
}

// get returns the tracked stream for key, or nil.
func (r *logStreamRegistry) get(key string) *logStream {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.streams[key]
}

// remove cancels and forgets the stream for key.
func (r *logStreamRegistry) remove(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.streams[key]; ok {
		s.cancel()
		delete(r.streams, key)
	}
}

// stopJob cancels and forgets every stream belonging to the BuildJob identified
// by namespace and jobID. Matching on the namespaced key prefix keeps same-named
// BuildJobs in different namespaces isolated.
func (r *logStreamRegistry) stopJob(namespace, jobID string) {
	prefix := logStreamJobPrefix(namespace, jobID)
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, s := range r.streams {
		if strings.HasPrefix(key, prefix) {
			s.cancel()
			delete(r.streams, key)
		}
	}
}
