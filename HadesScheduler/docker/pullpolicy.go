package docker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// PullPolicy decides whether a step contacts the registry for an image that is
// already in the local daemon.
//
// Why this exists: the executor used to call ImagePull unconditionally, once
// per step. Docker treats that as a registry round trip even when every layer
// is already present - it re-resolves the tag, fetches the manifest and checks
// each layer - so a job whose images were all local still paid for the network.
// Measured on a 3-step job over alpine:3.21 (one layer, 4 MB) that was 2.23s
// per step, about 63% of the executor's total per-job overhead, for images the
// daemon had already reported present. Larger images cost more, because the
// check is per layer: the same probe against a 15-layer image is markedly
// slower.
//
// The values mirror Kubernetes' imagePullPolicy, so the two executors can be
// reasoned about the same way.
type PullPolicy string

const (
	// PullAlways contacts the registry for every step, even when the image is
	// present locally. This is the default because it is what the executor has
	// always done: a mutable tag (`:latest`, or a branch tag that moves) is
	// refetched on every job, and silently changing that would mean a job
	// suddenly running yesterday's image. Correct, and the slowest.
	PullAlways PullPolicy = "always"

	// PullIfNotPresent pulls only when the image is not already in the local
	// daemon. Fast, and safe for immutable references (a digest, or a tag that
	// is never moved). Unsafe for mutable tags: once the image is cached
	// nothing will ever refresh it, so a moved tag is never picked up.
	PullIfNotPresent PullPolicy = "if-not-present"

	// PullNever never contacts the registry. A step whose image is absent fails
	// immediately rather than pulling it. For air-gapped hosts, and for
	// benchmarks that need image provisioning kept strictly outside the
	// measured interval.
	PullNever PullPolicy = "never"
)

// ParsePullPolicy validates a policy string.
//
// Deliberately strict: an unrecognised value is an error rather than a silent
// fallback to the default. A typo'd policy that quietly means "always" would
// present as "the setting does nothing", which is materially harder to
// diagnose than a refusal at startup.
func ParsePullPolicy(s string) (PullPolicy, error) {
	switch PullPolicy(s) {
	case PullAlways, PullIfNotPresent, PullNever:
		return PullPolicy(s), nil
	case "":
		return PullAlways, nil
	default:
		return "", fmt.Errorf(
			"invalid image pull policy %q: expected one of %q, %q, %q",
			s, PullAlways, PullIfNotPresent, PullNever)
	}
}

// shouldPull reports whether a step with this policy must contact the registry,
// given whether the image is already present locally.
func (p PullPolicy) shouldPull(presentLocally bool) bool {
	switch p {
	case PullNever:
		return false
	case PullIfNotPresent:
		return !presentLocally
	default: // PullAlways, and any zero value reaching here.
		return true
	}
}

// sharedPullTimeout bounds one coalesced pull. The pull runs detached from the
// job that started it, so it needs its own limit.
const sharedPullTimeout = 30 * time.Minute

// pullCoalescer shares image pulls across the jobs of one scheduler. With
// several concurrent workers, jobs using the same image would otherwise each
// send their own ImagePull, every one a full registry round trip that also
// counts against registry rate limits. A job that needs an image already being
// pulled waits for that pull instead of starting another.
//
// It also remembers when each image was last pulled successfully, so a
// freshness window can let back-to-back jobs skip the registry. With a zero
// window, which is the default, every job still pulls under PullAlways.
type pullCoalescer struct {
	freshness time.Duration

	mu       sync.Mutex
	inflight map[string]*pendingImage
	pulledAt map[string]time.Time
}

func newPullCoalescer(freshness time.Duration) *pullCoalescer {
	return &pullCoalescer{
		freshness: freshness,
		inflight:  make(map[string]*pendingImage),
		pulledAt:  make(map[string]time.Time),
	}
}

// pull joins the in-flight pull of image or starts one. The pull runs on a
// detached context: if the job that started it is cancelled, other jobs waiting
// on the same pull must not fail with it. A waiter whose own ctx ends stops
// waiting without cancelling the pull. A nil coalescer just pulls, so a
// Scheduler built as a struct literal still works.
func (c *pullCoalescer) pull(ctx context.Context, image string, pull func(context.Context) error) error {
	if c == nil {
		return pull(ctx)
	}
	c.mu.Lock()
	call, ok := c.inflight[image]
	if !ok {
		call = &pendingImage{done: make(chan struct{})}
		c.inflight[image] = call
		go func() {
			pullCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedPullTimeout)
			defer cancel()
			call.err = pull(pullCtx)
			c.mu.Lock()
			delete(c.inflight, image)
			// Only the freshness check reads pulledAt, so without a window
			// there is nothing to record. Expired entries are pruned here
			// because per-commit tags and digests would otherwise grow the
			// map for the lifetime of the scheduler.
			if call.err == nil && c.freshness > 0 {
				now := time.Now()
				for img, at := range c.pulledAt {
					if now.Sub(at) >= c.freshness {
						delete(c.pulledAt, img)
					}
				}
				c.pulledAt[image] = now
			}
			c.mu.Unlock()
			close(call.done)
		}()
	}
	c.mu.Unlock()

	select {
	case <-call.done:
		return call.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// recentlyPulled reports whether image was pulled successfully within the
// freshness window.
func (c *pullCoalescer) recentlyPulled(image string) bool {
	if c == nil || c.freshness <= 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	at, ok := c.pulledAt[image]
	return ok && time.Since(at) < c.freshness
}

// isDigestPinned reports whether ref names an image by digest. A digest is
// immutable, so a locally present copy is always the right one and pulling it
// again can only cost time.
func isDigestPinned(ref string) bool {
	return strings.Contains(ref, "@")
}
