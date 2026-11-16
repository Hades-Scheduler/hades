package nats

import "testing"

func TestSetupNatsConnectionRejectsHalfCredentials(t *testing.T) {
	for _, c := range []ConnectionConfig{
		{URL: "nats://127.0.0.1:1", Username: "hades"},
		{URL: "nats://127.0.0.1:1", Password: "secret"},
	} {
		if _, err := SetupDefaultNatsConnection(c); err == nil || err.Error() != "NATS_USERNAME and NATS_PASSWORD must be set together" {
			t.Errorf("config %+v: got %v", c, err)
		}
	}
}
