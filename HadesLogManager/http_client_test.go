package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loopbackTransport allows 127.0.0.0/8 so tests can post to httptest servers.
func loopbackTransport(t *testing.T) *http.Transport {
	t.Helper()
	allow, err := parseCallbackAllowlist([]string{"127.0.0.0/8"})
	require.NoError(t, err)
	return newCallbackTransport(0, allow)
}

func loopbackClient(t *testing.T) *http.Client {
	t.Helper()
	return &http.Client{Timeout: httpClientTimeout, Transport: loopbackTransport(t), CheckRedirect: noRedirect}
}

func TestCallbackControlBlocksInternalAddresses(t *testing.T) {
	allow, err := parseCallbackAllowlist(nil)
	require.NoError(t, err)

	for _, addr := range []string{
		"127.0.0.1:80", "[::1]:80", "10.1.2.3:80", "172.16.0.1:80", "192.168.1.1:80",
		"[fc00::1]:80", "169.254.169.254:80", "[fe80::1]:80", "0.0.0.0:80",
		"224.0.0.1:80", "[::ffff:127.0.0.1]:80", "100.100.100.200:80",
	} {
		assert.ErrorIs(t, allow.control("tcp", addr, nil), errBlockedTarget, addr)
	}
	for _, addr := range []string{"93.184.216.34:443", "[2606:4700::1111]:443"} {
		assert.NoError(t, allow.control("tcp", addr, nil), addr)
	}
}

func TestCallbackAllowlistExemptsListedTargets(t *testing.T) {
	allow, err := parseCallbackAllowlist([]string{"10.0.0.0/8", " 192.168.1.5 ", "Artemis"})
	require.NoError(t, err)

	assert.NoError(t, allow.control("tcp", "10.9.9.9:80", nil))
	assert.NoError(t, allow.control("tcp", "192.168.1.5:80", nil))
	assert.ErrorIs(t, allow.control("tcp", "192.168.1.6:80", nil), errBlockedTarget)
	assert.ErrorIs(t, allow.control("tcp", "169.254.169.254:80", nil), errBlockedTarget)
	assert.True(t, allow.hosts["artemis"])
	assert.True(t, allow.allowsIP(netip.MustParseAddr("10.0.0.1")))
}

func TestParseCallbackAllowlistRejectsBadCIDR(t *testing.T) {
	_, err := parseCallbackAllowlist([]string{"10.0.0.0/99"})
	assert.Error(t, err)
}

func TestCallbackTransportBlocksLoopbackAndAllowsViaAllowlist(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
	}))
	defer srv.Close()

	blocked, err := parseCallbackAllowlist(nil)
	require.NoError(t, err)
	client := &http.Client{Timeout: 5 * time.Second, Transport: newCallbackTransport(0, blocked)}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, nil)
	_, err = client.Do(req)
	require.ErrorIs(t, err, errBlockedTarget)
	assert.Equal(t, int32(0), atomic.LoadInt32(&hits))

	req, _ = http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, nil)
	resp, err := loopbackClient(t).Do(req)
	require.NoError(t, err)
	drainAndClose(resp)
	assert.Equal(t, int32(1), atomic.LoadInt32(&hits))
}
