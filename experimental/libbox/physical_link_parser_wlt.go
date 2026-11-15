//go:build with_wlt

package libbox

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/sagernet/sing/common/control"
)

// Linux netlink ABI values, independent of the host OS for parser tests.
const (
	physicalNetlinkHeaderLen = 16
	physicalIfInfomsgLen     = 16
	physicalRtAttrHeaderLen  = 4
	physicalIflaIfname       = 3
	physicalRTMNewLink       = 16
	physicalRTMDelLink       = 17
	physicalIFFUp            = 1
	physicalIFFLowerUp       = 0x10000
)

func parsePhysicalNetlinkMessages(data []byte) ([]physicalLinkUpdate, error) {
	var updates []physicalLinkUpdate
	if len(data) == 0 {
		return nil, fmt.Errorf("empty netlink datagram")
	}
	for len(data) > 0 {
		if len(data) < physicalNetlinkHeaderLen {
			return nil, fmt.Errorf("short netlink header")
		}
		// nlmsg_len is u32, nlmsg_type is u16 at byte offset four.
		length := uint64(binary.NativeEndian.Uint32(data[0:4]))
		messageType := binary.NativeEndian.Uint16(data[4:6])
		if length < physicalNetlinkHeaderLen || length > uint64(len(data)) {
			return nil, fmt.Errorf("invalid netlink length")
		}
		message := data[physicalNetlinkHeaderLen:int(length)]
		switch messageType {
		case 2: // NLMSG_ERROR, signed errno followed by original request header.
			if len(message) < 4 {
				return nil, fmt.Errorf("short netlink error")
			}
			if code := int32(binary.NativeEndian.Uint32(message[:4])); code != 0 {
				return nil, fmt.Errorf("netlink error code=%d", code)
			}
		case 4: // NLMSG_OVERRUN means the timeline is incomplete.
			return nil, fmt.Errorf("netlink overrun")
		case physicalRTMNewLink, physicalRTMDelLink:
			if len(message) < physicalIfInfomsgLen {
				return nil, fmt.Errorf("short ifinfomsg")
			}
			index := int(int32(binary.NativeEndian.Uint32(message[4:8])))
			if index <= 0 {
				return nil, fmt.Errorf("invalid link index")
			}
			flags := binary.NativeEndian.Uint32(message[8:12])
			change := binary.NativeEndian.Uint32(message[12:16])
			name := ""
			nameSeen := false
			attrs := message[physicalIfInfomsgLen:]
			for len(attrs) > 0 {
				if len(attrs) < physicalRtAttrHeaderLen {
					return nil, fmt.Errorf("short link attribute header")
				}
				attrLen := int(binary.NativeEndian.Uint16(attrs[:2]))
				attrType := binary.NativeEndian.Uint16(attrs[2:4]) & 0x3fff
				if attrLen < physicalRtAttrHeaderLen || attrLen > len(attrs) {
					return nil, fmt.Errorf("invalid link attribute length")
				}
				if attrType == physicalIflaIfname {
					value := attrs[4:attrLen]
					if nameSeen || len(value) < 2 || len(value) > 16 || value[len(value)-1] != 0 || bytes.IndexByte(value[:len(value)-1], 0) >= 0 {
						return nil, fmt.Errorf("invalid interface name attribute")
					}
					for _, c := range value[:len(value)-1] {
						if c <= 32 || c == 127 {
							return nil, fmt.Errorf("unsafe interface name attribute")
						}
					}
					name, nameSeen = string(value[:len(value)-1]), true
				}
				step := (attrLen + 3) &^ 3
				if step > len(attrs) {
					if attrLen != len(attrs) {
						return nil, fmt.Errorf("truncated link attribute padding")
					}
					step = attrLen
				}
				attrs = attrs[step:]
			}
			updates = append(updates, physicalLinkUpdate{MessageType: messageType, InterfaceIndex: index, InterfaceName: name, RawFlags: flags, Change: change, AdminUp: flags&physicalIFFUp != 0, LowerUp: flags&physicalIFFLowerUp != 0, Deleted: messageType == physicalRTMDelLink})
		}
		step := (length + 3) &^ 3
		if step > uint64(len(data)) {
			if length != uint64(len(data)) {
				return nil, fmt.Errorf("truncated netlink padding")
			}
			step = length
		}
		data = data[int(step):]
	}
	return updates, nil
}

type physicalLinkDatagram struct {
	Data       []byte
	ObservedAt time.Time
	Current    *control.Interface
}

// One goroutine owns receive, parsing and diagnostic delivery. Socket close
// interrupts the runtime poller; waiting for stopped also waits for the last
// diagnostic callback. No extra queue can strand a producer during shutdown.
type physicalLinkReader struct {
	done      chan struct{}
	stopped   chan struct{}
	interrupt func()
	closeOnce sync.Once
}

func startPhysicalLinkReader(startedAt time.Time, receive func() (physicalLinkDatagram, error), interrupt func(), emit func(physicalLinkObservation), reportError func(error)) *physicalLinkReader {
	r := &physicalLinkReader{done: make(chan struct{}), stopped: make(chan struct{}), interrupt: interrupt}
	go func() {
		defer close(r.stopped)
		for {
			select {
			case <-r.done:
				return
			default:
			}
			packet, err := receive()
			if err != nil {
				select {
				case <-r.done:
					return
				default:
				}
				reportError(err)
				return // No retry storm; the run retains an incomplete timeline.
			}
			updates, err := parsePhysicalNetlinkMessages(packet.Data)
			if err != nil {
				reportError(err)
				return
			}
			for _, update := range updates {
				emit(observePhysicalLinkUpdate(startedAt, packet.ObservedAt, update, packet.Current))
			}
		}
	}()
	return r
}

func (r *physicalLinkReader) close() {
	r.closeOnce.Do(func() { close(r.done); r.interrupt() })
	<-r.stopped
}
