//go:build with_wlt

package wlt

import "time"

// Caller read deadlines are independent of idle expiry. The existing carrier
// idle watchdog owns scoped idle closure and its read handoff. Internal activity
// never creates, clears or extends an application read deadline.
// Writes keep their existing minimum of caller and idle deadlines.
type carrierApplicationDeadlinePolicy struct{ read, write, idle time.Time }

func earlierCarrierDeadline(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}
func (c *carrierConn) SetDeadline(d time.Time) error {
	if !c.scopedApplicationClose {
		return c.Conn.SetDeadline(d)
	}
	c.applicationDeadlineMu.Lock()
	defer c.applicationDeadlineMu.Unlock()
	if e := c.Conn.SetDeadline(d); e != nil {
		return e
	}
	// The original caller operation already applied both caps. Preserve that
	// authority even if the subsequent write-only idle rebalance fails.
	c.applicationDeadline.read = d
	c.applicationDeadline.write = d
	write := earlierCarrierDeadline(d, c.applicationDeadline.idle)
	if !write.Equal(d) {
		return c.Conn.SetWriteDeadline(write)
	}
	return nil
}
func (c *carrierConn) SetReadDeadline(d time.Time) error {
	if !c.scopedApplicationClose {
		return c.Conn.SetReadDeadline(d)
	}
	c.applicationDeadlineMu.Lock()
	defer c.applicationDeadlineMu.Unlock()
	e := c.Conn.SetReadDeadline(d)
	if e == nil {
		c.applicationDeadline.read = d
	}
	return e
}
func (c *carrierConn) SetWriteDeadline(d time.Time) error {
	if !c.scopedApplicationClose {
		return c.Conn.SetWriteDeadline(d)
	}
	c.applicationDeadlineMu.Lock()
	defer c.applicationDeadlineMu.Unlock()
	effective := earlierCarrierDeadline(d, c.applicationDeadline.idle)
	err := c.Conn.SetWriteDeadline(effective)
	if err == nil {
		c.applicationDeadline.write = d
	}
	return err
}
func (c *carrierConn) refreshScopedApplicationDeadline() {
	c.applicationDeadlineMu.Lock()
	defer c.applicationDeadlineMu.Unlock()
	idle := time.Now().Add(c.idleTimeout)
	c.applicationDeadline.idle = idle
	_ = c.Conn.SetWriteDeadline(earlierCarrierDeadline(c.applicationDeadline.write, idle))
}
