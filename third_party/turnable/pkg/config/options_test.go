package config

import "testing"

func TestApplyEnvironmentOptionsAdaptivePeerAndKCP(t *testing.T) {
	previous := Options
	defer func() {
		Options = previous
	}()
	Options = GlobalOptions{}
	t.Setenv("TURNABLE_ADAPTIVE_PEER_DATA", "1")
	t.Setenv("TURNABLE_ADAPTIVE_PEER_THRESHOLD_BYTES_PER_SECOND", "2097152")
	t.Setenv("TURNABLE_ADAPTIVE_PEER_IDLE_MILLIS", "8000")
	t.Setenv("TURNABLE_KCP_WINDOW", "1024")
	t.Setenv("TURNABLE_KCP_BUFFER", "2097152")

	ApplyEnvironmentOptions()

	transport := Options.Transport
	if !transport.AdaptivePeerData ||
		transport.AdaptivePeerThresholdBytes != 2097152 ||
		transport.AdaptivePeerIdleMillis != 8000 ||
		transport.KCPWindowSize != 1024 ||
		transport.KCPReadWriteBuffer != 2097152 {
		t.Fatalf("transport options=%+v", transport)
	}
}
