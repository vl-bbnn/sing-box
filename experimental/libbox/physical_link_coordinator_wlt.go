//go:build with_wlt

package libbox

import (
	"github.com/sagernet/sing/common/control"
	E "github.com/sagernet/sing/common/exceptions"
	"sync"
)

// physicalLinkCoordinator is installed only by Android's with_wlt hook. Its
// mutex protects state, never platform calls, transport cancellation or callbacks.
// Recovery publication waits for loss cancellation and the current callback
// delivery. Loss is allowed to preempt either a refresh or callback delivery.
type physicalLinkCoordinator struct {
	mu         sync.Mutex
	monitor    *platformDefaultInterfaceMonitor
	revision   uint64
	selected   *control.Interface
	blocked    *control.Interface
	deleted    bool
	closed     bool
	losses     int
	delivering bool
	notify     bool
	ready      *physicalLinkReady
	stopReader func()
	joinReader func()
	workers    sync.WaitGroup
	inCallback bool
}
type physicalLinkReady struct {
	revision uint64
	iface    *control.Interface
}

func newPhysicalLinkCoordinator(m *platformDefaultInterfaceMonitor) *physicalLinkCoordinator {
	return &physicalLinkCoordinator{monitor: m}
}
func samePhysicalInterface(a, b *control.Interface) bool {
	return a != nil && b != nil && a.Index == b.Index && a.Name == b.Name
}
func copyPhysicalInterface(a *control.Interface) *control.Interface {
	if a == nil {
		return nil
	}
	return &control.Interface{Index: a.Index, Name: a.Name}
}
func (c *physicalLinkCoordinator) currentLocked() *control.Interface {
	return c.monitor.DefaultInterface()
}
func (c *physicalLinkCoordinator) publishLocked(iface *control.Interface) {
	c.monitor.defaultInterfaceAccess.Lock()
	c.monitor.defaultInterface = iface
	c.monitor.defaultInterfaceAccess.Unlock()
	c.notify = true
}
func (c *physicalLinkCoordinator) snapshot() (*control.Interface, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, c.revision
	}
	current := c.currentLocked()
	if current == nil && c.blocked != nil {
		current = c.selected
	}
	return copyPhysicalInterface(current), c.revision
}
func (c *physicalLinkCoordinator) update(name string, index int32, expensive, constrained bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.revision++
	revision := c.revision
	c.ready = nil
	c.monitor.defaultInterfaceAccess.Lock()
	c.monitor.isExpensive = expensive
	c.monitor.isConstrained = constrained
	c.monitor.defaultInterfaceAccess.Unlock()
	if index == -1 {
		c.selected = nil
		c.blocked = nil
		c.deleted = false
		c.publishLocked(nil)
		c.losses++
		c.mu.Unlock()
		c.cancelLossPriority()
		c.startWorker(c.completeLoss)
		c.refreshInterfaces()
		return
	}
	c.selected = &control.Interface{Index: int(index), Name: name}
	c.blocked = nil
	c.deleted = false
	target := copyPhysicalInterface(c.selected)
	c.mu.Unlock()
	c.refresh(revision, target)
}
func (c *physicalLinkCoordinator) refreshInterfaces() error {
	err := c.monitor.networkManager.UpdateInterfaces()
	if err != nil {
		c.monitor.logger.Error(E.Cause(err, "update interfaces"))
	}
	return err
}
func (c *physicalLinkCoordinator) refresh(revision uint64, target *control.Interface) {
	if c.refreshInterfaces() != nil {
		return
	}
	iface, err := c.monitor.networkManager.InterfaceFinder().ByIndex(target.Index)
	if err != nil {
		c.monitor.logger.Error(E.Cause(err, "find updated interface: ", target.Name))
		return
	}
	// Never accept a cached/reused index belonging to a different interface.
	if !samePhysicalInterface(iface, target) {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if revision != c.revision || !samePhysicalInterface(target, c.selected) {
		// Loss of the old underlay must not strand a distinct, already-selected
		// replacement whose earlier refresh was invalidated by that loss.
		retry := samePhysicalInterface(target, c.selected) && c.blocked != nil && !samePhysicalInterface(c.blocked, c.selected)
		nextRevision := c.revision
		c.mu.Unlock()
		if retry {
			c.refresh(nextRevision, target)
		}
		return
	}
	c.ready = &physicalLinkReady{revision: revision, iface: iface}
	c.mu.Unlock()
	c.deliver()
}
func (c *physicalLinkCoordinator) forward(event physicalLinkObservation) bool {
	c.mu.Lock()
	if c.closed || event.InterfaceRevision != c.revision || !event.CurrentMatch || event.InterfaceName == "" {
		c.mu.Unlock()
		return false
	}
	identity := &control.Interface{Index: event.InterfaceIndex, Name: event.InterfaceName}
	if !event.CandidateLoss {
		// Netlink only revalidates the retained ConnectivityManager selection.
		// A deletion can reuse even the same name/index; it requires a new platform
		// callback. An authoritative nil selection cannot be restored by link-up.
		if event.Deleted || !event.AdminUp || !event.LowerUp || c.deleted || !samePhysicalInterface(c.blocked, identity) || !samePhysicalInterface(c.selected, identity) {
			c.mu.Unlock()
			return false
		}
		c.revision++
		revision := c.revision
		target := copyPhysicalInterface(c.selected)
		c.mu.Unlock()
		c.startWorker(func() { c.refresh(revision, target) })
		return false
	}
	current := c.currentLocked()
	// While a prior loss is still being delivered the exposed default is nil,
	// but the retained identity remains authoritative for matching subsequent
	// native loss datagrams. Do not discard that immediate cancellation edge.
	if current == nil && c.blocked != nil {
		current = c.blocked
	}
	if !samePhysicalInterface(current, identity) {
		c.mu.Unlock()
		return false
	}
	for _, name := range c.monitor.MyInterfaces() {
		if name == current.Name {
			c.mu.Unlock()
			return false
		}
	}
	c.revision++
	c.ready = nil
	c.blocked = copyPhysicalInterface(current)
	c.deleted = event.Deleted
	c.publishLocked(nil)
	c.losses++
	c.mu.Unlock()
	c.cancelLossPriority()
	c.startWorker(c.completeLoss)
	return true
}

// cancelLossPriority is deliberately synchronous: the structural WLT admission
// close must happen before the observer reports the loss as handled. Ordinary
// callback delivery is completed by a worker so a blocked callback cannot hold
// up the netlink reader and a later native loss.
func (c *physicalLinkCoordinator) cancelLossPriority() {
	// This lane runs before ordinary callback housekeeping, including when an
	// earlier recovery callback is blocked. No generic listeners are invoked.
	if priority, ok := c.monitor.networkManager.(interface{ NotifyPhysicalNetworkUnavailable() }); ok {
		priority.NotifyPhysicalNetworkUnavailable()
	}
}
func (c *physicalLinkCoordinator) completeLoss() {
	c.mu.Lock()
	if c.losses > 0 {
		c.losses--
	}
	c.mu.Unlock()
	c.deliver()
}
func (c *physicalLinkCoordinator) startWorker(fn func()) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.workers.Add(1)
	c.mu.Unlock()
	go func() { defer c.workers.Done(); fn() }()
}
func (c *physicalLinkCoordinator) deliver() {
	c.mu.Lock()
	if c.closed || c.delivering || c.losses != 0 {
		c.mu.Unlock()
		return
	}
	c.delivering = true
	for {
		if c.closed || c.losses != 0 {
			c.delivering = false
			c.mu.Unlock()
			return
		}
		if ready := c.ready; ready != nil {
			c.ready = nil
			if ready.revision == c.revision {
				if !samePhysicalInterface(c.currentLocked(), ready.iface) {
					c.publishLocked(ready.iface)
				}
				c.blocked = nil
				c.deleted = false
			}
		}
		if !c.notify {
			c.delivering = false
			c.mu.Unlock()
			return
		}
		c.notify = false
		revision := c.revision
		current := c.currentLocked()
		c.monitor.defaultInterfaceAccess.Lock()
		callbacks := c.monitor.callbacks.Array()
		c.monitor.defaultInterfaceAccess.Unlock()
		c.mu.Unlock()
		for _, callback := range callbacks {
			c.mu.Lock()
			valid := !c.closed && c.revision == revision
			c.mu.Unlock()
			if !valid {
				break
			}
			c.mu.Lock()
			if c.closed || c.revision != revision {
				c.mu.Unlock()
				break
			}
			c.inCallback = true
			c.mu.Unlock()
			callback(current, 0)
			c.mu.Lock()
			c.inCallback = false
			c.mu.Unlock()
		}
		c.mu.Lock()
	}
}
func (c *physicalLinkCoordinator) attachReader(stop func(), join ...func()) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		stop()
		return
	}
	c.stopReader = stop
	if len(join) > 0 {
		c.joinReader = join[0]
	}
	c.mu.Unlock()
}
func (c *physicalLinkCoordinator) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.revision++
	c.ready = nil
	c.notify = false
	stop := c.stopReader
	c.stopReader = nil
	join := c.joinReader
	c.joinReader = nil
	inCallback := c.inCallback
	c.mu.Unlock()
	// Close may be reentrant from delivery on the reader goroutine. Interrupt
	// without waiting on that same goroutine; its owner can separately join it.
	if stop != nil {
		stop()
	}
	if join != nil && !inCallback {
		join()
	}
	if !inCallback {
		c.workers.Wait()
	}
}
