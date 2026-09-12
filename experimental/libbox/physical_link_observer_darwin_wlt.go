//go:build darwin && with_wlt

package libbox

import (
	"os"
	"sync"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	"golang.org/x/net/route"
	"golang.org/x/sys/unix"
)

type darwinLinkEvent struct {
	kind  int
	index int
	flags int
}

func darwinLinkEvents(messages []route.Message) []darwinLinkEvent {
	var events []darwinLinkEvent
	for _, message := range messages {
		switch m := message.(type) {
		case *route.InterfaceMessage:
			if m.Type == unix.RTM_IFINFO {
				events = append(events, darwinLinkEvent{m.Type, m.Index, m.Flags})
			}
		case *route.InterfaceAddrMessage:
			if m.Type == unix.RTM_DELADDR || m.Type == unix.RTM_NEWADDR {
				events = append(events, darwinLinkEvent{m.Type, m.Index, m.Flags})
			}
		case *route.RouteMessage:
			if m.Err == nil && (m.Type == unix.RTM_DELETE || m.Type == unix.RTM_ADD || m.Type == unix.RTM_CHANGE) {
				events = append(events, darwinLinkEvent{m.Type, m.Index, m.Flags})
			}
		}
	}
	return events
}

// Observe kernel delivery only. No route writes, polling, interface publication
// or WLT cancellation is allowed here until physical timing is established.
func startPhysicalLinkObserver(m *platformDefaultInterfaceMonitor) (func(), error) {
	fd, err := unix.Socket(unix.AF_ROUTE, unix.SOCK_RAW, 0)
	if err != nil {
		return nil, err
	}
	unix.CloseOnExec(fd)
	if err = unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	socket := os.NewFile(uintptr(fd), "wlt-darwin-route-observer")
	return runDarwinLinkObserver(m, socket), nil
}

func runDarwinLinkObserver(m *platformDefaultInterfaceMonitor, socket *os.File) func() {
	done := make(chan struct{})
	var once sync.Once
	m.logger.Info("darwin physical link observer started diagnostic_only=true")
	go func() {
		defer close(done)
		defer socket.Close()
		buffer := make([]byte, 64*1024)
		var sequence uint64
		for {
			n, err := socket.Read(buffer)
			observed := carriercommon.DarwinUptimeNanos()
			if err != nil {
				return
			}
			messages, err := route.ParseRIB(route.RIBTypeRoute, buffer[:n])
			if err != nil {
				m.logger.Warn("darwin physical link observer parse_failed diagnostic_only=true")
				continue
			}
			for _, event := range darwinLinkEvents(messages) {
				sequence++
				m.logger.Info("darwin physical link event sequence=", sequence,
					" read_uptime_ns=", observed, " kind=", event.kind,
					" index=", event.index, " flags=", event.flags,
					" diagnostic_only=true")
			}
		}
	}()
	return func() {
		once.Do(func() { _ = socket.Close() })
		<-done
		m.logger.Info("darwin physical link observer stopped diagnostic_only=true")
	}
}
