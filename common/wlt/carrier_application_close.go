//go:build with_wlt

package wlt

import "net"

// Full application close retires unread inbound DATA through the original
// receiver. Legacy transports retain Close, and Abort keeps its immediate path.
// Server pipe half-close does not use this client-only capability.
func closeApplicationStream(conn net.Conn) error {
	if closer, ok := conn.(interface{ CloseApplication() error }); ok {
		return closer.CloseApplication()
	}
	return conn.Close()
}
