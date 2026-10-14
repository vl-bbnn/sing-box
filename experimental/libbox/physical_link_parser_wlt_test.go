//go:build with_wlt

package libbox

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing/common/control"
)

// A literal Linux arm64 nlmsghdr + ifinfomsg + IFLA_IFNAME fixture, not
// serialized through the parser's offsets/constants. nlmsg_len=48 (u32),
// nlmsg_type=16 (u16 at offset 4), index=9, flags=UP|LOWER_UP.
func physicalABIFrame(t *testing.T) []byte {
	t.Helper()
	b, err := hex.DecodeString("300000001000000001000000000000000000000009000000010001000000010010000300726d6e65745f646174613000")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPhysicalParserKernelABI(t *testing.T) {
	b := physicalABIFrame(t)
	u, err := parsePhysicalNetlinkMessages(b)
	if err != nil || len(u) != 1 {
		t.Fatalf("updates=%+v err=%v", u, err)
	}
	if got := u[0]; got.MessageType != 16 || got.InterfaceIndex != 9 || got.InterfaceName != "rmnet_data0" || got.RawFlags != 0x10001 || got.Change != 0x10000 || !got.AdminUp || !got.LowerUp || got.Deleted {
		t.Fatalf("wrong ABI decode: %+v", got)
	}
	other := append([]byte(nil), b...)
	other[4] = 17
	other[24] = 0
	other[26] = 0
	u, err = parsePhysicalNetlinkMessages(append(b, other...))
	if err != nil || len(u) != 2 || !u[1].Deleted || u[1].AdminUp || u[1].LowerUp {
		t.Fatalf("multi/deletion updates=%+v err=%v", u, err)
	}
	// Upper nlmsg_len bits must not be discarded as in the rejected draft.
	b[2] = 1
	if _, err = parsePhysicalNetlinkMessages(b); err == nil {
		t.Fatal("accepted truncated u32 netlink length")
	}
}

func TestPhysicalParserInvalidFrames(t *testing.T) {
	base := physicalABIFrame(t)
	cases := map[string][]byte{"empty": nil, "short_header": base[:15], "short_body": base[:31]}
	for name, edit := range map[string]func([]byte){
		"short_length":       func(b []byte) { b[0] = 8 },
		"overlong_length":    func(b []byte) { b[0] = 49 },
		"zero_index":         func(b []byte) { b[20] = 0 },
		"short_attribute":    func(b []byte) { b[32] = 2 },
		"overlong_attribute": func(b []byte) { b[32] = 17 },
		"empty_name":         func(b []byte) { b[36] = 0 },
		"unterminated_name":  func(b []byte) { b[47] = 'x' },
		"newline_name":       func(b []byte) { b[38] = '\n' },
	} {
		b := append([]byte(nil), base...)
		edit(b)
		cases[name] = b
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			u, err := parsePhysicalNetlinkMessages(b)
			if err == nil || len(u) != 0 {
				t.Fatalf("accepted malformed frame: %+v %v", u, err)
			}
		})
	}
	// An invalid later message cannot leave a partially accepted datagram.
	if u, err := parsePhysicalNetlinkMessages(append(base, 1, 2, 3)); err == nil || u != nil {
		t.Fatal("partial datagram accepted")
	}
}

func TestPhysicalParserOptionalAndControlMessages(t *testing.T) {
	b := physicalABIFrame(t)
	// Unknown/nested attributes need no decoder or sysfs fallback.
	b[34] = 18
	b[35] = 0x80
	u, err := parsePhysicalNetlinkMessages(b)
	if err != nil || len(u) != 1 || u[0].InterfaceName != "" {
		t.Fatalf("unknown attr: %+v %v", u, err)
	}
	b = physicalABIFrame(t)
	b[35] = 0x80
	u, err = parsePhysicalNetlinkMessages(b)
	if err != nil || len(u) != 1 || u[0].InterfaceName != "rmnet_data0" {
		t.Fatalf("attribute flag: %+v %v", u, err)
	}
	for _, kind := range []byte{1, 3, 20} {
		b := make([]byte, 16)
		b[0] = 16
		b[4] = kind
		if u, err := parsePhysicalNetlinkMessages(b); err != nil || len(u) != 0 {
			t.Fatalf("non-link %d: %+v %v", kind, u, err)
		}
	}
	ack := make([]byte, 20)
	ack[0] = 20
	ack[4] = 2
	if _, err := parsePhysicalNetlinkMessages(ack); err != nil {
		t.Fatal(err)
	}
	binary.NativeEndian.PutUint32(ack[16:20], 0xffffffff)
	if _, err := parsePhysicalNetlinkMessages(ack); err == nil {
		t.Fatal("kernel error ignored")
	}
	ack[0] = 16
	ack[4] = 4
	if _, err := parsePhysicalNetlinkMessages(ack[:16]); err == nil {
		t.Fatal("overrun ignored")
	}
}

func TestPhysicalReaderRetainsReceiveSnapshotAndDrainsCallback(t *testing.T) {
	frame := physicalABIFrame(t)
	started := time.Unix(100, 0)
	received := started.Add(20 * time.Millisecond)
	entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var reads, interrupts atomic.Int32
	observations := make(chan physicalLinkObservation, 1)
	r := startPhysicalLinkReader(started, func() (physicalLinkDatagram, error) {
		reads.Add(1)
		return physicalLinkDatagram{Data: frame, ObservedAt: received, Current: &control.Interface{Index: 9, Name: "rmnet_data0"}}, nil
	}, func() { interrupts.Add(1) }, func(o physicalLinkObservation) { close(entered); <-release; observations <- o }, func(err error) { t.Error(err) })
	<-entered
	go func() { r.close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("close returned before callback")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("close stalled")
	}
	r.close()
	o := <-observations
	if reads.Load() != 1 || interrupts.Load() != 1 || !o.CurrentMatch || o.CurrentInterfaceIndex != 9 || o.MonotonicNanos != 20_000_000 || o.WallUnixMillis != received.UnixMilli() {
		t.Fatalf("receive snapshot lost: %+v reads=%d interrupts=%d", o, reads.Load(), interrupts.Load())
	}
}

func TestPhysicalReaderInterruptAndFailClosed(t *testing.T) {
	for _, mode := range []string{"close", "receive_error", "parse_error"} {
		t.Run(mode, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var errorsSeen, reads atomic.Int32
			r := startPhysicalLinkReader(time.Now(), func() (physicalLinkDatagram, error) {
				reads.Add(1)
				close(entered)
				<-release
				if mode == "parse_error" {
					return physicalLinkDatagram{Data: []byte{1, 2}}, nil
				}
				return physicalLinkDatagram{}, errors.New("injected receive failure")
			}, func() {
				if mode == "close" {
					close(release)
				}
			}, func(physicalLinkObservation) { t.Error("emitted invalid record") }, func(error) { errorsSeen.Add(1) })
			<-entered
			if mode == "close" {
				r.close()
			} else {
				close(release)
				select {
				case <-r.stopped:
				case <-time.After(5 * time.Second):
					t.Fatal("reader retried error")
				}
				r.close()
			}
			want := int32(1)
			if mode == "close" {
				want = 0
			}
			if errorsSeen.Load() != want || reads.Load() != 1 {
				t.Fatalf("error storm/close errors: %d %d", errorsSeen.Load(), reads.Load())
			}
		})
	}
}
