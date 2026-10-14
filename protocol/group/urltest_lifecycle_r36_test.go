package group

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// Box starts inbounds before outbound PostStart. Touch from early traffic can
// therefore run while PostStart publishes started. One PostStart per group;
// no network, provider, device, or repeated lifecycle call is involved.
func TestURLTestPostStartConcurrentTouchR36(t *testing.T) {
	for iteration := 0; iteration < 50; iteration++ {
		ticker := time.NewTicker(time.Hour)
		g := &URLTestGroup{ticker: ticker}
		// Suppress probe work, which is independent of the lifecycle field under
		// test. The real PostStart path still schedules CheckOutbounds as usual.
		g.checking.Store(true)
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Add(3)
		go func() { defer workers.Done(); <-start; g.PostStart() }()
		for worker := 0; worker < 2; worker++ {
			go func() {
				defer workers.Done()
				<-start
				for n := 0; n < 100; n++ {
					g.Touch()
					runtime.Gosched()
				}
			}()
		}
		close(start)
		workers.Wait()
		ticker.Stop()
	}
}
