//go:build with_wlt && wlt_deadline_fixture

package wlt

import (
	"net"
	"sync"
	"testing"
	"time"
)

// Test-only scheduling barrier. No production hook or source field is added.
func DeadlineFixtureHoldIdleWatch4541(t *testing.T, c *Carrier, stream net.Conn) (time.Time, func()) {
	t.Helper()
	c.streamsMu.Lock()
	raw, ok := stream.(*carrierConn)
	if !ok {
		c.streamsMu.Unlock()
		t.Fatal("original carrier stream missing")
	}
	last := raw.lastActivity.Load()
	if last == 0 {
		c.streamsMu.Unlock()
		t.Fatal("last original activity missing")
	}
	deadline := time.Unix(0, last).Add(raw.idleTimeout)
	var once sync.Once
	release := func() { once.Do(c.streamsMu.Unlock) }
	t.Cleanup(release)
	return deadline, release
}
