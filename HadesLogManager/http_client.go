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
// Loopback, private, link-local, unspecified, multicast and special-purpose
// addresses are refused unless listed in AllowedHosts.
type CallbackConfig struct {
	// AllowedHosts is a comma-separated list of host names, IPs or CIDRs
	// (e.g. "artemis,10.0.0.0/8") that callbacks may reach despite the block.
	AllowedHosts []string `env:"CALLBACK_ALLOWED_HOSTS" envSeparator:","`
	// BlockedCIDRs adds ranges to refuse, for clusters whose pod or service
	// network uses addresses that look public. An AllowedHosts entry still
	// wins over it.
	BlockedCIDRs []string `env:"CALLBACK_BLOCKED_CIDRS" envSeparator:","`
}

// errBlockedTarget marks a callback target the dialer refused. Retrying cannot
// fix it, so callers treat it as permanent.
var errBlockedTarget = errors.New("callback target is not allowed")

// specialPurpose lists the non-global ranges the net.IP helpers do not cover.
// Clusters often use them for pod and service networks.
var specialPurpose = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),       // "this network"
	netip.MustParsePrefix("100.64.0.0/10"),   // CGNAT (RFC 6598), some clouds serve metadata here
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),    // documentation (TEST-NET-1)
	netip.MustParsePrefix("192.88.99.0/24"),  // deprecated 6to4 relay anycast
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking, used for pod and service networks
	netip.MustParsePrefix("198.51.100.0/24"), // documentation (TEST-NET-2)
	netip.MustParsePrefix("203.0.113.0/24"),  // documentation (TEST-NET-3)
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved, includes broadcast
	netip.MustParsePrefix("64:ff9b::/96"),    // NAT64, maps to an IPv4 target
	netip.MustParsePrefix("64:ff9b:1::/48"),  // local-use NAT64, maps to an IPv4 target
	netip.MustParsePrefix("100::/64"),        // discard-only
	netip.MustParsePrefix("2001::/32"),       // Teredo
	netip.MustParsePrefix("2001:db8::/32"),   // documentation
	netip.MustParsePrefix("2002::/16"),       // 6to4
}

// callbackAllowlist holds the operator's exemptions from the target block and
// any extra ranges to refuse.
type callbackAllowlist struct {
	hosts    map[string]bool
	prefixes []netip.Prefix
	blocked  []netip.Prefix
}

// blockCIDRs adds operator-configured CIDRs to the refused ranges.
func (a *callbackAllowlist) blockCIDRs(entries []string) error {
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		p, err := netip.ParsePrefix(e)
		if err != nil {
			return fmt.Errorf("invalid CALLBACK_BLOCKED_CIDRS entry %q", e)
		}
		a.blocked = append(a.blocked, p.Masked())
	}
	return nil
}

func isSpecialPurpose(ip netip.Addr, extra []netip.Prefix) bool {
	for _, list := range [][]netip.Prefix{specialPurpose, extra} {
		for _, p := range list {
			if p.Contains(ip) {
				return true
			}
		}
	}
	return false
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

// control refuses blocked ranges, but an allowlist entry wins over both the
// built-in list and CALLBACK_BLOCKED_CIDRS. It runs after DNS resolution, once per connection attempt, so DNS
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
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsMulticast() || isSpecialPurpose(ip, a.blocked) {
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
