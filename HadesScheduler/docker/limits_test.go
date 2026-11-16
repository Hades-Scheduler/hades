package docker

import (
	"testing"

	"github.com/hades-scheduler/hades/shared/payload"
	"github.com/stretchr/testify/require"
)

func TestCheckNetworks(t *testing.T) {
	opts := Options{allowedNetworks: []string{"none", "bridge", "default"}}
	for _, tc := range []struct {
		network string
		ok      bool
	}{
		{"", true},
		{"none", true},
		{"bridge", true},
		{"default", true},
		{"host", false},
		{"hades_default", false},
	} {
		err := opts.checkNetworks([]payload.Step{{ID: 1}, {ID: 2, Network: tc.network}})
		if tc.ok {
			require.NoError(t, err, tc.network)
		} else {
			require.ErrorContains(t, err, "step 2")
			require.ErrorContains(t, err, tc.network)
		}
	}
}

func TestWithAllowedNetworksTrimsAndExtends(t *testing.T) {
	s := &Scheduler{}
	require.NoError(t, WithAllowedNetworks([]string{" none", "hades_jobs ", ""})(s))
	require.NoError(t, s.checkNetworks([]payload.Step{{Network: "hades_jobs"}}))
	require.Error(t, s.checkNetworks([]payload.Step{{Network: "bridge"}}))
}

func TestJobExecuteRejectsDisallowedNetworkBeforeAnyWork(t *testing.T) {
	// A nil client would panic if execute got as far as pulling an image.
	job := Job{
		Options:      Options{allowedNetworks: []string{"none"}},
		QueuePayload: payload.QueuePayload{Steps: []payload.Step{{ID: 1, Image: "x", Network: "host"}}},
	}
	require.ErrorContains(t, job.execute(t.Context()), `network "host" is not allowed`)
}

func TestEffectiveTimeout(t *testing.T) {
	for _, tc := range []struct {
		name               string
		def, max, in, want int64
	}{
		{"off keeps payload", 0, 0, 30, 30},
		{"off keeps zero", 0, 0, 0, 0},
		{"default fills zero", 60, 0, 0, 60},
		{"default does not override", 60, 0, 30, 30},
		{"max caps larger", 0, 100, 500, 100},
		{"max keeps smaller", 0, 100, 50, 50},
		{"max applies to zero", 0, 100, 0, 100},
		{"default above max is capped", 200, 100, 0, 100},
	} {
		got := Options{defaultJobTimeout: tc.def, maxJobTimeout: tc.max}.effectiveTimeout(tc.in)
		require.Equal(t, tc.want, got, tc.name)
	}
}

func TestWithJobTimeoutsRejectsNegative(t *testing.T) {
	require.Error(t, WithJobTimeouts(-1, 0)(&Scheduler{}))
	require.NoError(t, WithJobTimeouts(10, 20)(&Scheduler{}))
}
