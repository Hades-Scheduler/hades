package docker

import (
	"context"
	"errors"
	"sync"
	"testing"
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
