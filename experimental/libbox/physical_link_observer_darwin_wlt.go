//go:build darwin && with_wlt

package libbox

import (
	"net/netip"
	"os"
	"sync"

	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
	"golang.org/x/net/route"
	"golang.org/x/sys/unix"
)

type darwinLinkEvent struct {
	kind    int
	index   int
	flags   int
	address netip.Addr
}

func darwinLinkEvents(messages []route.Message) []darwinLinkEvent {
	var events []darwinLinkEvent
	for _, message := range messages {
		switch m := message.(type) {
		case *route.InterfaceMessage:
			if m.Type == unix.RTM_IFINFO {
				events = append(events, darwinLinkEvent{kind: m.Type, index: m.Index, flags: m.Flags})
			}
		case *route.InterfaceAddrMessage:
			if m.Type == unix.RTM_DELADDR || m.Type == unix.RTM_NEWADDR {
				event := darwinLinkEvent{kind: m.Type, index: m.Index, flags: m.Flags}
				if len(m.Addrs) > unix.RTAX_IFA {
					if address, ok := m.Addrs[unix.RTAX_IFA].(*route.Inet4Addr); ok {
						event.address = netip.AddrFrom4(address.IP)
					}
				}
				events = append(events, event)
			}
		case *route.RouteMessage:
			if m.Err == nil && (m.Type == unix.RTM_DELETE || m.Type == unix.RTM_ADD || m.Type == unix.RTM_CHANGE) {
				events = append(events, darwinLinkEvent{kind: m.Type, index: m.Index, flags: m.Flags})
			}
		}
	}
	return events
}

// Kernel address withdrawal precedes NWPath notification on iOS. This reader
// may retire the exact current IPv4 interface, but never publishes a new path.
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
	m.logger.Info("darwin physical link observer started loss_on_current_ipv4_withdrawal=true")
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
				retired := m.retireDarwinInterface(event)
				m.logger.Info("darwin physical link event sequence=", sequence,
					" read_uptime_ns=", observed, " kind=", event.kind,
					" index=", event.index, " flags=", event.flags,
					" current_interface_retired=", retired)
			}
		}
	}()
	return func() {
		once.Do(func() { _ = socket.Close() })
		<-done
		m.logger.Info("darwin physical link observer stopped diagnostic_only=true")
	}
}

func (m *platformDefaultInterfaceMonitor) retireDarwinInterface(event darwinLinkEvent) bool {
	m.defaultInterfaceAccess.Lock()
	addressReturned := event.kind == unix.RTM_NEWADDR && event.address.Is4() && !event.address.IsUnspecified()
	linkReturned := m.wltRetiredInterfaceByLink && event.kind == unix.RTM_IFINFO && event.flags&unix.IFF_UP != 0
	if (addressReturned || linkReturned) && m.wltRetiredInterfaceIndex == event.index {
		m.wltRetiredInterfaceIndex = 0
		m.wltRetiredInterfaceByLink = false
	}
	current := m.defaultInterface
	if current == nil || current.Index != event.index {
		m.defaultInterfaceAccess.Unlock()
		return false
	}
	// IPv6 privacy-address rotation and arbitrary route deletion are not loss
	// evidence for the IPv4 TURN socket. Require deletion of an address that
	// the current interface actually owns, or an administrative link-down.
	lost := event.kind == unix.RTM_IFINFO && event.flags&unix.IFF_UP == 0
	if event.kind == unix.RTM_DELADDR && event.address.Is4() {
		for _, prefix := range current.Addresses {
			if prefix.Addr().Unmap() == event.address {
				lost = true
				break
			}
		}
	}
	if !lost {
		m.defaultInterfaceAccess.Unlock()
		return false
	}
	m.wltHandoverEpoch.Add(1)
	m.wltRetiredInterfaceIndex = current.Index
	m.wltRetiredInterfaceByLink = event.kind == unix.RTM_IFINFO
	m.defaultInterface = nil
	dispatched := make(chan struct{})
	m.wltKernelLossDone = dispatched
	callbacks := m.callbacks.Array()
	m.defaultInterfaceAccess.Unlock()
	defer func() {
		m.defaultInterfaceAccess.Lock()
		m.wltKernelLossDone = nil
		close(dispatched)
		m.defaultInterfaceAccess.Unlock()
	}()
	for _, callback := range callbacks {
		callback(nil, 0)
	}
	return true
}
