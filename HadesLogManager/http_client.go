package main

import (
	"io"
	"net/http"
	"time"
)

// minIdleConnsPerHost is the keep-alive pool floor per callback host. Both
// callbacks typically go to one adapter, and Go's default of two idle
// connections per host would make most concurrent deliveries dial afresh.
const minIdleConnsPerHost = 32

// maxDrainedResponseBytes bounds how much of a callback response body is read
// before closing it. Neither callback parses the body.
const maxDrainedResponseBytes = 64 << 10

// newCallbackTransport returns the transport shared by the log callback and the
// status webhook, so both reuse one connection pool. maxConcurrent is the
// status webhook concurrency; the per-host idle pool is sized to cover it.
func newCallbackTransport(maxConcurrent int) *http.Transport {
	perHost := max(maxConcurrent, minIdleConnsPerHost)
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = perHost
	t.MaxIdleConns = max(t.MaxIdleConns, 4*perHost)
	t.IdleConnTimeout = 90 * time.Second
	return t
}

// drainAndClose reads a bounded prefix of resp's body before closing it, so the
// connection can go back to the keep-alive pool instead of being torn down on
// every delivery. The limit keeps a chatty receiver from making the read
// unbounded.
func drainAndClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainedResponseBytes))
	_ = resp.Body.Close()
}
