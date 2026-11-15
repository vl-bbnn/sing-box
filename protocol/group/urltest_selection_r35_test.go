package group

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/common/urltest"
)

// Exercises the actual background selection publisher and the group-status
// reader concurrently. No sockets, physical devices, or provider traffic.
func TestURLTestSelectionConcurrentNowR35(t *testing.T) {
	a := &failoverTestOutbound{tag: "primary"}
	b := &failoverTestOutbound{tag: "secondary"}
	history := urltest.NewHistoryStorage()
	history.StoreURLTestHistory(b.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 20})
	g := &URLTestGroup{
		outbounds:            []adapter.Outbound{a, b},
		history:              history,
		preferFirstAvailable: true,
		interruptGroup:       interrupt.NewGroup(),
	}
	g.performUpdateCheck()
	o := &URLTest{group: g}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 3000; i++ {
			if i%2 == 0 {
				history.StoreURLTestHistory(a.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 10})
			} else {
				history.DeleteURLTestHistory(a.Tag())
			}
			g.performUpdateCheck()
			runtime.Gosched()
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 3000; i++ {
			value := o.Now()
			if value != a.Tag() && value != b.Tag() {
				t.Errorf("invalid selected tag %q", value)
			}
			runtime.Gosched()
		}
	}()
	close(start)
	workers.Wait()
}
