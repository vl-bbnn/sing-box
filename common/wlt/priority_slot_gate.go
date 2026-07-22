//go:build with_wlt

package wlt

import (
	"context"
	"sync"
)

// prioritySlotGate keeps a borrowable reserve for priority work. Either class
// may use the full capacity while the other class has no waiters. Under mixed
// pressure each class retains its configured share, and FIFO order is kept
// among simultaneously eligible waiters.
type prioritySlotGate struct {
	mu sync.Mutex

	capacity int
	reserved int
	total    int
	priority int
	normal   int
	nextSeq  uint64
	waiters  []*prioritySlotWaiter
}

type prioritySlotWaiter struct {
	priority bool
	seq      uint64
	ready    chan struct{}
	granted  bool
}

func newPrioritySlotGate(capacity int, reserved int) *prioritySlotGate {
	return &prioritySlotGate{capacity: capacity, reserved: reserved}
}

func (g *prioritySlotGate) tryAcquire(priority bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.waiters) != 0 || !g.canGrantLocked(priority, false, false) {
		return false
	}
	g.grantClassLocked(priority)
	return true
}

func (g *prioritySlotGate) acquire(ctx context.Context, priority bool) error {
	g.mu.Lock()
	if len(g.waiters) == 0 && g.canGrantLocked(priority, false, false) {
		g.grantClassLocked(priority)
		g.mu.Unlock()
		return nil
	}
	waiter := &prioritySlotWaiter{
		priority: priority,
		seq:      g.nextSeq,
		ready:    make(chan struct{}),
	}
	g.nextSeq++
	g.waiters = append(g.waiters, waiter)
	g.dispatchLocked()
	if waiter.granted {
		g.mu.Unlock()
		return nil
	}
	g.mu.Unlock()

	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		if waiter.granted {
			g.mu.Unlock()
			return nil
		}
		g.removeWaiterLocked(waiter)
		g.dispatchLocked()
		g.mu.Unlock()
		return ctx.Err()
	}
}

func (g *prioritySlotGate) release(priority bool) {
	g.mu.Lock()
	if g.total > 0 {
		g.total--
		if priority && g.priority > 0 {
			g.priority--
		} else if !priority && g.normal > 0 {
			g.normal--
		}
	}
	g.dispatchLocked()
	g.mu.Unlock()
}

func (g *prioritySlotGate) dispatchLocked() {
	for g.total < g.capacity && len(g.waiters) > 0 {
		hasPriority, hasNormal := g.waiterClassesLocked()
		selected := -1
		for index, waiter := range g.waiters {
			if !g.canGrantLocked(waiter.priority, hasPriority, hasNormal) {
				continue
			}
			if selected == -1 || waiter.seq < g.waiters[selected].seq {
				selected = index
			}
		}
		if selected == -1 {
			return
		}
		waiter := g.waiters[selected]
		g.waiters = append(g.waiters[:selected], g.waiters[selected+1:]...)
		g.grantClassLocked(waiter.priority)
		waiter.granted = true
		close(waiter.ready)
	}
}

func (g *prioritySlotGate) canGrantLocked(priority bool, hasPriorityWaiter bool, hasNormalWaiter bool) bool {
	if g.total >= g.capacity {
		return false
	}
	if priority {
		return g.priority < g.reserved || !hasNormalWaiter
	}
	return g.normal < g.capacity-g.reserved || !hasPriorityWaiter
}

func (g *prioritySlotGate) grantClassLocked(priority bool) {
	g.total++
	if priority {
		g.priority++
	} else {
		g.normal++
	}
}

func (g *prioritySlotGate) waiterClassesLocked() (bool, bool) {
	var priority, normal bool
	for _, waiter := range g.waiters {
		if waiter.priority {
			priority = true
		} else {
			normal = true
		}
	}
	return priority, normal
}

func (g *prioritySlotGate) removeWaiterLocked(target *prioritySlotWaiter) {
	for index, waiter := range g.waiters {
		if waiter == target {
			g.waiters = append(g.waiters[:index], g.waiters[index+1:]...)
			return
		}
	}
}
