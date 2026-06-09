package connection

import "testing"

func TestIsBenignPlatformEnd(t *testing.T) {
	cases := []struct {
		reason string
		want   bool
	}{
		{"platform signaling ended", true},
		{"websocket: close 1000 (normal): Bye", true},
		{"platform signaling ended: websocket: close 1000 (normal): Bye", true},
		{"websocket: close 1006 (abnormal closure): unexpected EOF", false},
		{"all peers disconnected", false},
	}

	for _, tc := range cases {
		if got := isBenignPlatformEnd(tc.reason); got != tc.want {
			t.Fatalf("isBenignPlatformEnd(%q) = %v, want %v", tc.reason, got, tc.want)
		}
	}
}
