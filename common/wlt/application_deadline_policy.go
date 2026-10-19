//go:build with_wlt

package wlt

import "time"

// Explicit caller deadlines and the carrier idle limit are separate authorities.
// This policy is selected only by scoped_application_read_retirement. Ordinary
// and default WLT retain the original direct forwarding paths.
type carrierApplicationDeadlinePolicy struct {
	read, write, idle time.Time
}

func earlierCarrierDeadline(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}
func (c *carrierConn) setEffectiveReadDeadline(d time.Time, idle bool) error {
	if idle {
		if reporter, ok := c.Conn.(interface{ SetIdleReadDeadline(time.Time) error }); ok {
			return reporter.SetIdleReadDeadline(d)
		}
	}
	return c.Conn.SetReadDeadline(d)
}
func (c *carrierConn) setEffectiveBothDeadline(d time.Time, idle bool) error {
	if idle {
		if reporter, ok := c.Conn.(interface{ SetIdleDeadline(time.Time) error }); ok {
			return reporter.SetIdleDeadline(d)
		}
	}
	return c.Conn.SetDeadline(d)
}

func (c *carrierConn) SetDeadline(d time.Time) error {
	if !c.scopedApplicationClose {
		return c.Conn.SetDeadline(d)
	}
	c.applicationDeadlineMu.Lock()
	defer c.applicationDeadlineMu.Unlock()
	effective := earlierCarrierDeadline(d, c.applicationDeadline.idle)
	err := c.setEffectiveBothDeadline(effective, !c.applicationDeadline.idle.IsZero() && effective.Equal(c.applicationDeadline.idle))
	if err == nil {
		c.applicationDeadline.read = d
		c.applicationDeadline.write = d
	}
	return err
}
func (c *carrierConn) SetReadDeadline(d time.Time) error {
	if !c.scopedApplicationClose {
		return c.Conn.SetReadDeadline(d)
	}
	c.applicationDeadlineMu.Lock()
	defer c.applicationDeadlineMu.Unlock()
	effective := earlierCarrierDeadline(d, c.applicationDeadline.idle)
	err := c.setEffectiveReadDeadline(effective, !c.applicationDeadline.idle.IsZero() && effective.Equal(c.applicationDeadline.idle))
	if err == nil {
		c.applicationDeadline.read = d
	}
	return err
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
	read := earlierCarrierDeadline(c.applicationDeadline.read, idle)
	write := earlierCarrierDeadline(c.applicationDeadline.write, idle)
	if read.Equal(write) {
		_ = c.setEffectiveBothDeadline(read, read.Equal(idle))
		return
	}
	_ = c.setEffectiveReadDeadline(read, read.Equal(idle))
	_ = c.Conn.SetWriteDeadline(write)
}
