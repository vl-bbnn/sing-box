//go:build with_wlt && wlt_deadline_fixture

package wlt

import (
	"context"
	"net"
	"testing"
	"time"
)

// Export only from the test binary: production source is byte-exact to main690.
func DeadlineFixtureCarrier4479(t *testing.T, conn net.Conn, idle time.Duration) (*Carrier, net.Conn) {
	t.Helper()
	c := newTestCarrier(1, 1, 1, time.Second, time.Second)
	c.done = make(chan struct{})
	c.idleTimeout = idle
	c.scopedReadRetirement = true
	c.dialRoute = func(context.Context, string) (net.Conn, error) { return conn, nil }
	stream, e := c.DialStream(context.Background(), "eu", "example.com:443")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = stream.Close(); close(c.done); assertScopedAdmissionReleased(t, c) })
	return c, stream.(net.Conn)
}

func DeadlineFixturePriorSuffix4479(conn any, numbers map[string]int64) string {
	return applicationPriorReadCauseSuffix(conn, numbers)
}
