package interrupt

import (
	"io"
	"net"
	"sync"

	"github.com/sagernet/sing/common/x/list"
)

type Group struct {
	access      sync.Mutex
	connections list.List[*groupConnItem]
}

type groupConnItem struct {
	conn       io.Closer
	isExternal bool
	closeOnce  sync.Once
	closeErr   error
}

func (c *groupConnItem) close() error {
	c.closeOnce.Do(func() { c.closeErr = c.conn.Close() })
	return c.closeErr
}

func NewGroup() *Group {
	return &Group{}
}

func (g *Group) NewConn(conn net.Conn, isExternal bool) net.Conn {
	g.access.Lock()
	defer g.access.Unlock()
	item := g.connections.PushBack(&groupConnItem{conn: conn, isExternal: isExternal})
	return &Conn{Conn: conn, group: g, element: item}
}

func (g *Group) NewPacketConn(conn net.PacketConn, isExternal bool) net.PacketConn {
	g.access.Lock()
	defer g.access.Unlock()
	item := g.connections.PushBack(&groupConnItem{conn: conn, isExternal: isExternal})
	return &PacketConn{PacketConn: conn, group: g, element: item}
}

// Interrupt detaches the selected generation under the group lock. Network
// teardown can block (TLS close-notify, stream drains); it must not prevent
// newly established connections from being published to their callers.
func (g *Group) Interrupt(interruptExternalConnections bool) {
	g.access.Lock()
	var closing []*groupConnItem
	for element := g.connections.Front(); element != nil; {
		next := element.Next()
		if !element.Value.isExternal || interruptExternalConnections {
			closing = append(closing, element.Value)
			g.connections.Remove(element)
		}
		element = next
	}
	g.access.Unlock()
	for _, item := range closing {
		_ = item.close()
	}
}
