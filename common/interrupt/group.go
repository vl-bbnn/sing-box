package interrupt

import (
	"io"
	"net"
 "github.com/sagernet/sing/common/bufio"
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
}

func NewGroup() *Group {
	return &Group{}
}

func (g *Group) NewConn(conn net.Conn, isExternal bool) net.Conn {
	g.access.Lock()
	defer g.access.Unlock()
	item := g.connections.PushBack(&groupConnItem{conn, isExternal})
	return &Conn{Conn: conn, group: g, element: item}
}

func (g *Group) NewPacketConn(conn net.PacketConn, isExternal bool) net.PacketConn {
	g.access.Lock()
	defer g.access.Unlock()
	item := g.connections.PushBack(&groupConnItem{conn, isExternal})
	return &PacketConn{NetPacketConn: bufio.NewPacketConn(conn), group: g, element: item}
}

func (g *Group) Interrupt(interruptExternalConnections bool) {
	g.access.Lock()
	var toClose []io.Closer
	for element := g.connections.Front(); element != nil; {
		next := element.Next()
		if !element.Value.isExternal || interruptExternalConnections {
			toClose = append(toClose, element.Value.conn)
			g.connections.Remove(element)
		}
		element = next
	}
	g.access.Unlock()
	for _, conn := range toClose {
		_ = conn.Close()
	}
}
