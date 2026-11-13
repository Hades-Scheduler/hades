package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"syscall"
	"time"
)

// CallbackConfig configures which callback targets the log manager may reach.
// Loopback, private, link-local, unspecified and multicast addresses are
// refused unless listed here.
type CallbackConfig struct {
	// AllowedHosts is a comma-separated list of host names, IPs or CIDRs
	// (e.g. "artemis,10.0.0.0/8") that callbacks may reach despite the block.
	AllowedHosts []string `env:"CALLBACK_ALLOWED_HOSTS" envSeparator:","`
}

// errBlockedTarget marks a callback target the dialer refused. Retrying cannot
// fix it, so callers treat it as permanent.
var errBlockedTarget = errors.New("callback target is not allowed")

// cgnat is the shared address space (RFC 6598). It is not public and some
// clouds serve instance metadata from it.
var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// callbackAllowlist holds the operator's exemptions from the target block.
type callbackAllowlist struct {
	hosts    map[string]bool
	prefixes []netip.Prefix
}

func parseCallbackAllowlist(entries []string) (*callbackAllowlist, error) {
	a := &callbackAllowlist{hosts: map[string]bool{}}
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if p, err := netip.ParsePrefix(e); err == nil {
			a.prefixes = append(a.prefixes, p.Masked())
		} else if ip, err := netip.ParseAddr(e); err == nil {
			a.prefixes = append(a.prefixes, netip.PrefixFrom(ip.Unmap(), ip.Unmap().BitLen()))
		} else if strings.Contains(e, "/") {
			return nil, fmt.Errorf("invalid CALLBACK_ALLOWED_HOSTS entry %q", e)
		} else {
			a.hosts[strings.ToLower(e)] = true
		}
	}
	return a, nil
}

func (a *callbackAllowlist) allowsIP(ip netip.Addr) bool {
	for _, p := range a.prefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// control runs after DNS resolution, once per connection attempt, so DNS
// rebinding and redirects cannot reach a blocked address.
func (a *callbackAllowlist) control(_, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("%w: %v", errBlockedTarget, err)
	}
	ip := ap.Addr().Unmap()
	if a.allowsIP(ip) {
		return nil
	}
	if ip.IsLoopback() || ip.IsPrivate() || cgnat.Contains(ip) || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsMulticast() {
		return fmt.Errorf("%w: %s", errBlockedTarget, ip)
	}
	return nil
}

// noRedirect returns the 3xx to the caller instead of following it.
func noRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

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
// Connections to blocked targets fail unless allow exempts them. Proxies are
// not used, since a proxy would do the dialing and bypass the check.
func newCallbackTransport(maxConcurrent int, allow *callbackAllowlist) *http.Transport {
	perHost := max(maxConcurrent, minIdleConnsPerHost)
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	guarded := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second, Control: allow.control}
	open := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if host, _, err := net.SplitHostPort(addr); err == nil && allow.hosts[strings.ToLower(host)] {
			return open.DialContext(ctx, network, addr)
		}
		return guarded.DialContext(ctx, network, addr)
	}
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
