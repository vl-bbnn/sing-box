//go:build with_wlt

package libbox

import (
	"sync"
	"time"

	"github.com/sagernet/sing/common/control"
)

// physicalLinkUpdate is deliberately independent of the Linux netlink parser
// so matching, timing and lifecycle behavior can be host tested.
type physicalLinkUpdate struct {
	ObservedAt              time.Time
	CurrentCaptured         bool
	CurrentInterfaceAtEvent *control.Interface
	MessageType             uint16
	InterfaceIndex          int
	InterfaceName           string
	RawFlags                uint32
	Change                  uint32
	AdminUp                 bool
	LowerUp                 bool
	Deleted                 bool
}

type physicalLinkObservation struct {
	MonotonicNanos        int64
	WallUnixMillis        int64
	MessageType           uint16
	InterfaceIndex        int
	InterfaceName         string
	RawFlags              uint32
	Change                uint32
	AdminUp               bool
	LowerUp               bool
	Deleted               bool
	CurrentInterfaceIndex int
	CurrentInterfaceName  string
	CurrentMatch          bool
	CandidateLoss         bool
}

func observePhysicalLinkUpdate(startedAt time.Time, observedAt time.Time, update physicalLinkUpdate, current *control.Interface) physicalLinkObservation {
	observation := physicalLinkObservation{
		MonotonicNanos:        observedAt.Sub(startedAt).Nanoseconds(),
		WallUnixMillis:        observedAt.UnixMilli(),
		MessageType:           update.MessageType,
		InterfaceIndex:        update.InterfaceIndex,
		InterfaceName:         update.InterfaceName,
		RawFlags:              update.RawFlags,
		Change:                update.Change,
		AdminUp:               update.AdminUp,
		LowerUp:               update.LowerUp,
		Deleted:               update.Deleted,
		CurrentInterfaceIndex: -1,
	}
	if current != nil {
		observation.CurrentInterfaceIndex = current.Index
		observation.CurrentInterfaceName = current.Name
		observation.CurrentMatch = current.Index == update.InterfaceIndex
		if update.InterfaceName != "" && current.Name != "" {
			observation.CurrentMatch = observation.CurrentMatch && current.Name == update.InterfaceName
		}
	}
	observation.CandidateLoss = observation.CurrentMatch && (update.Deleted || !update.AdminUp || !update.LowerUp)
	return observation
}

// physicalLinkObservationLoop owns only diagnostic delivery. Closing it never
// updates the platform monitor and never signals WLT carrier state.
type physicalLinkObservationLoop struct {
	startedAt time.Time
	now       func() time.Time
	current   func() *control.Interface
	emit      func(physicalLinkObservation)
	done      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
}

func newPhysicalLinkObservationLoop(startedAt time.Time, now func() time.Time, current func() *control.Interface, emit func(physicalLinkObservation)) *physicalLinkObservationLoop {
	return &physicalLinkObservationLoop{
		startedAt: startedAt,
		now:       now,
		current:   current,
		emit:      emit,
		done:      make(chan struct{}),
		stopped:   make(chan struct{}),
	}
}

func (o *physicalLinkObservationLoop) run(updates <-chan physicalLinkUpdate) {
	defer close(o.stopped)
	for {
		select {
		case <-o.done:
			return
		case update, open := <-updates:
			if !open {
				return
			}
			observedAt := update.ObservedAt
			if observedAt.IsZero() {
				observedAt = o.now()
			}
			current := update.CurrentInterfaceAtEvent
			if !update.CurrentCaptured {
				current = o.current()
			}
			o.emit(observePhysicalLinkUpdate(o.startedAt, observedAt, update, current))
		}
	}
}

func (o *physicalLinkObservationLoop) close() {
	o.closeOnce.Do(func() { close(o.done) })
	<-o.stopped
}
