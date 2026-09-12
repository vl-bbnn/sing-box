//go:build darwin && with_wlt

package libbox

import (
	"context"
	"errors"
	"net/netip"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/control"
	"golang.org/x/net/route"
	"golang.org/x/sys/unix"
)

func TestDarwinLinkEventsDoNotExposeAddressesOrTreatQueriesAsLoss(t *testing.T) {
	messages := []route.Message{
		&route.InterfaceMessage{Type: unix.RTM_IFINFO, Index: 16, Flags: unix.IFF_UP},
		&route.InterfaceAddrMessage{Type: unix.RTM_DELADDR, Index: 16},
		&route.RouteMessage{Type: unix.RTM_GET, Index: 16},
		&route.RouteMessage{Type: unix.RTM_DELETE, Index: 16, Err: errors.New("denied")},
		&route.RouteMessage{Type: unix.RTM_DELETE, Index: 16},
	}
	want := []darwinLinkEvent{{kind: unix.RTM_IFINFO, index: 16, flags: unix.IFF_UP}, {kind: unix.RTM_DELADDR, index: 16}, {kind: unix.RTM_DELETE, index: 16}}
	if got := darwinLinkEvents(messages); !reflect.DeepEqual(got, want) {
		t.Fatalf("events=%v want=%v", got, want)
	}
}

func TestDarwinAddressWithdrawalRetiresOnlyExactIPv4Generation(t *testing.T) {
	address := netip.MustParseAddr("192.0.2.4")
	old := &control.Interface{Index: 16, Name: "wifi", Addresses: []netip.Prefix{netip.PrefixFrom(address, 24)}}
	m := &platformDefaultInterfaceMonitor{platformInterfaceWrapper: &platformInterfaceWrapper{defaultInterface: old}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	losses := 0
	m.RegisterCallback(func(iface *control.Interface, _ int) {
		if iface != nil {
			t.Fatal("kernel observer published a replacement")
		}
		if m.DefaultInterface() != nil {
			t.Fatal("old interface still published")
		}
		losses++
		cancel()
	})
	for _, event := range []darwinLinkEvent{
		{kind: unix.RTM_DELADDR, index: 2, address: address},
		{kind: unix.RTM_DELADDR, index: 16, address: netip.MustParseAddr("192.0.2.5")},
		{kind: unix.RTM_DELADDR, index: 16, address: netip.MustParseAddr("2001:db8::1")},
		{kind: unix.RTM_DELETE, index: 16, address: address},
	} {
		if m.retireDarwinInterface(event) {
			t.Fatal("unrelated event retired current path")
		}
	}
	event := darwinLinkEvent{kind: unix.RTM_DELADDR, index: 16, address: address}
	if !m.retireDarwinInterface(event) || ctx.Err() == nil || losses != 1 {
		t.Fatal("loss did not cancel once")
	}
	if m.retireDarwinInterface(event) || losses != 1 {
		t.Fatal("duplicate loss delivered")
	}
	if !wltInterfacePublicationBlocked(m, 16) || wltInterfacePublicationBlocked(m, 2) {
		t.Fatal("stale path gate mismatch")
	}
	m.retireDarwinInterface(darwinLinkEvent{kind: unix.RTM_IFINFO, index: 16, flags: unix.IFF_UP})
	if !wltInterfacePublicationBlocked(m, 16) {
		t.Fatal("link-up reopened a withdrawn address")
	}
	m.retireDarwinInterface(darwinLinkEvent{kind: unix.RTM_NEWADDR, index: 16, address: address})
	if wltInterfacePublicationBlocked(m, 16) {
		t.Fatal("address return did not release publication")
	}
}

func TestDarwinLinkUpReleasesAdministrativeLossGate(t *testing.T) {
	m := &platformDefaultInterfaceMonitor{platformInterfaceWrapper: &platformInterfaceWrapper{defaultInterface: &control.Interface{Index: 16}}}
	if !m.retireDarwinInterface(darwinLinkEvent{kind: unix.RTM_IFINFO, index: 16}) {
		t.Fatal("link-down not retired")
	}
	m.retireDarwinInterface(darwinLinkEvent{kind: unix.RTM_IFINFO, index: 16, flags: unix.IFF_UP})
	if wltInterfacePublicationBlocked(m, 16) {
		t.Fatal("administrative link-up did not release gate")
	}
}

func TestDarwinKernelLossDispatchPrecedesNewEpoch(t *testing.T) {
	m := &platformDefaultInterfaceMonitor{platformInterfaceWrapper: &platformInterfaceWrapper{defaultInterface: &control.Interface{Index: 16}}}
	entered, release, lost, epoch := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan uint64, 1)
	m.RegisterCallback(func(*control.Interface, int) { close(entered); <-release })
	go func() { m.retireDarwinInterface(darwinLinkEvent{kind: unix.RTM_IFINFO, index: 16}); close(lost) }()
	<-entered
	go func() { epoch <- beginWLTEpoch(m) }()
	select {
	case <-epoch:
		t.Fatal("new path epoch overtook cancellation dispatch")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-lost
	select {
	case value := <-epoch:
		if value != 2 {
			t.Fatalf("epoch=%d", value)
		}
	case <-time.After(time.Second):
		t.Fatal("new epoch remained blocked")
	}
}

func TestDarwinObserverCloseWakesBlockedReader(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	m := &platformDefaultInterfaceMonitor{logger: log.NewNOPFactory().Logger()}
	closeObserver := runDarwinLinkObserver(m, reader)
	done := make(chan struct{})
	go func() { closeObserver(); closeObserver(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("observer did not join its reader after Close")
	}
}

func TestDarwinIPv6DeletionChecksActualIPv4WithoutRetiringPrivacyRotation(t *testing.T) {
	m := &platformDefaultInterfaceMonitor{platformInterfaceWrapper: &platformInterfaceWrapper{defaultInterface: &control.Interface{Index: 16}}}
	event := darwinLinkEvent{kind: unix.RTM_DELADDR, index: 16, family: unix.AF_INET6}
	usable := m.verifyDarwinAddressLoss(event, func(int) (bool, error) { return true, nil })
	if m.retireDarwinInterface(usable) {
		t.Fatal("IPv6 rotation retired usable IPv4")
	}
	uncertain := m.verifyDarwinAddressLoss(event, func(int) (bool, error) { return false, errors.New("unavailable") })
	if m.retireDarwinInterface(uncertain) {
		t.Fatal("query failure treated as confirmed loss")
	}
	stale := m.verifyDarwinAddressLoss(event, func(int) (bool, error) { beginWLTEpoch(m); return false, nil })
	if m.retireDarwinInterface(stale) {
		t.Fatal("stale observation retired a new callback generation")
	}
	lost := m.verifyDarwinAddressLoss(event, func(int) (bool, error) { return false, nil })
	if !m.retireDarwinInterface(lost) {
		t.Fatal("confirmed IPv4 absence did not retire old carrier")
	}
}
