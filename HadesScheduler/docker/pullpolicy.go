package docker

import "fmt"

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
