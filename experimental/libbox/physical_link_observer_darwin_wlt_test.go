//go:build darwin && with_wlt

package libbox

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
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
	want := []darwinLinkEvent{{unix.RTM_IFINFO, 16, unix.IFF_UP}, {unix.RTM_DELADDR, 16, 0}, {unix.RTM_DELETE, 16, 0}}
	if got := darwinLinkEvents(messages); !reflect.DeepEqual(got, want) {
		t.Fatalf("events=%v want=%v", got, want)
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
