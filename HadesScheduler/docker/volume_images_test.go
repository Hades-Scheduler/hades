package docker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/stretchr/testify/require"
)

func TestRemoveVolumeWithRetry(t *testing.T) {
	inUse := errors.New("remove x: volume is in use - [abc]")

	t.Run("retries while in use then succeeds", func(t *testing.T) {
		calls := 0
		err := removeVolumeWithRetry(context.Background(), func(context.Context) error {
			calls++
			if calls < 3 {
				return inUse
			}
			return nil
		}, time.Millisecond, time.Second)
		require.NoError(t, err)
		require.Equal(t, 3, calls)
	})

	t.Run("other errors fail immediately", func(t *testing.T) {
		calls := 0
		boom := errors.New("boom")
		err := removeVolumeWithRetry(context.Background(), func(context.Context) error {
			calls++
			return boom
		}, time.Millisecond, time.Second)
		require.ErrorIs(t, err, boom)
		require.Equal(t, 1, calls)
	})

	t.Run("gives up after the window", func(t *testing.T) {
		calls := 0
		err := removeVolumeWithRetry(context.Background(), func(context.Context) error {
			calls++
			return inUse
		}, time.Millisecond, 20*time.Millisecond)
		require.ErrorIs(t, err, inUse)
		require.Greater(t, calls, 1)
	})

	t.Run("stops on context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := removeVolumeWithRetry(ctx, func(context.Context) error { return inUse }, time.Millisecond, time.Minute)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestDistinctImages(t *testing.T) {
	refs := distinctImages([]payload.Step{
		{ID: 1, Image: "a"}, {ID: 2, Image: "b"}, {ID: 3, Image: "a"}, {ID: 4, Image: "c"},
	})
	require.Equal(t, []imageRef{{"a", 1}, {"b", 2}, {"c", 4}}, refs)
}

func TestPrefetchHonoursPolicy(t *testing.T) {
	refs := []imageRef{{"local", 1}, {"remote", 2}}
	run := func(policy PullPolicy) (map[string]error, []string, map[int]bool) {
		var mu sync.Mutex
		var pulled []string
		recorded := map[int]bool{}
		p := imagePuller{
			policy:  policy,
			present: func(_ context.Context, img string) bool { return img == "local" },
			pull: func(_ context.Context, img string) error {
				mu.Lock()
				defer mu.Unlock()
				pulled = append(pulled, img)
				return nil
			},
			record: func(step int, _, _ time.Time, cached bool) {
				mu.Lock()
				defer mu.Unlock()
				recorded[step] = cached
			},
		}
		res := map[string]error{}
		for img, pi := range p.prefetch(context.Background(), refs) {
			res[img] = pi.wait()
		}
		return res, pulled, recorded
	}

	res, pulled, rec := run(PullAlways)
	require.ElementsMatch(t, []string{"local", "remote"}, pulled)
	require.NoError(t, res["local"])
	require.Equal(t, map[int]bool{1: true, 2: false}, rec)

	res, pulled, _ = run(PullIfNotPresent)
	require.Equal(t, []string{"remote"}, pulled)
	require.NoError(t, res["remote"])

	res, pulled, _ = run(PullNever)
	require.Empty(t, pulled)
	require.NoError(t, res["local"])
	require.ErrorContains(t, res["remote"], "not present locally")
}

func TestNeedsPullSkipsKnownResults(t *testing.T) {
	p := imagePuller{policy: PullAlways, recent: func(img string) bool { return img == "fresh:1" }}
	require.True(t, p.needsPull("alpine:3", true))
	require.False(t, p.needsPull("alpine@sha256:abc", true), "a present digest cannot change")
	require.True(t, p.needsPull("alpine@sha256:abc", false), "an absent digest still has to be pulled")
	require.False(t, p.needsPull("fresh:1", true))
	require.True(t, p.needsPull("fresh:1", false))
}

func TestPullCoalescerSharesInflightPull(t *testing.T) {
	// synctest.Wait lets the test know the second job is really parked on the
	// shared pull before it is released. Without it the second goroutine could
	// arrive after the first pull finished and start a pull of its own.
	synctest.Test(t, func(t *testing.T) {
		c := newPullCoalescer(time.Minute)
		release := make(chan struct{})
		var calls atomic.Int32
		pull := func(context.Context) error {
			calls.Add(1)
			<-release
			return nil
		}

		// The first job is cancelled while the pull runs; the second must still
		// get the shared result instead of the first job's cancellation.
		ctx1, cancel1 := context.WithCancel(context.Background())
		errs := make(chan error, 2)
		go func() { errs <- c.pull(ctx1, "img", pull) }()
		synctest.Wait()
		go func() { errs <- c.pull(context.Background(), "img", pull) }()
		synctest.Wait()
		cancel1()
		require.ErrorIs(t, <-errs, context.Canceled)
		require.False(t, c.recentlyPulled("img"))
		close(release)
		require.NoError(t, <-errs)

		require.Equal(t, int32(1), calls.Load())
		require.True(t, c.recentlyPulled("img"))
		require.False(t, newPullCoalescer(0).recentlyPulled("img"))
	})
}

func TestPullCoalescerDoesNotCacheFailures(t *testing.T) {
	c := newPullCoalescer(time.Minute)
	boom := errors.New("boom")
	require.ErrorIs(t, c.pull(context.Background(), "img", func(context.Context) error { return boom }), boom)
	require.False(t, c.recentlyPulled("img"))
	require.NoError(t, c.pull(context.Background(), "img", func(context.Context) error { return nil }))
}

func TestPullCoalescerPrunesExpiredPulls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ok := func(context.Context) error { return nil }
		c := newPullCoalescer(time.Minute)
		require.NoError(t, c.pull(context.Background(), "old", ok))
		time.Sleep(time.Minute)
		require.NoError(t, c.pull(context.Background(), "new", ok))
		require.NotContains(t, c.pulledAt, "old")
		require.Contains(t, c.pulledAt, "new")

		off := newPullCoalescer(0)
		require.NoError(t, off.pull(context.Background(), "img", ok))
		require.Empty(t, off.pulledAt, "nothing reads pull times without a freshness window")
	})
}
