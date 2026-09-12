//go:build with_wlt

package libbox

import (
	"testing"
	"time"

	"github.com/sagernet/sing/common/control"
)

func TestObservePhysicalLinkUpdateMatchesCurrentInterfaceAndKeepsMonotonicTime(t *testing.T) {
	startedAt := time.Unix(100, 0)
	observedAt := startedAt.Add(43 * time.Millisecond)
	observation := observePhysicalLinkUpdate(startedAt, observedAt, physicalLinkUpdate{
		MessageType:    16,
		InterfaceIndex: 9,
		InterfaceName:  "rmnet_data0",
		RawFlags:       0x1001,
		Change:         ^uint32(0),
		AdminUp:        true,
		LowerUp:        false,
	}, &control.Interface{Index: 9, Name: "rmnet_data0"})

	if observation.MonotonicNanos != int64(43*time.Millisecond) {
		t.Fatalf("monotonic nanos=%d", observation.MonotonicNanos)
	}
	if !observation.CurrentMatch || !observation.CandidateLoss {
		t.Fatalf("matching link down not classified: %+v", observation)
	}
	if !observation.AdminUp || observation.LowerUp || observation.Deleted {
		t.Fatalf("raw link state changed: %+v", observation)
	}
}

func TestObservePhysicalLinkUpdateRejectsNonCurrentAndNameMismatch(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		update  physicalLinkUpdate
		current *control.Interface
	}{
		{
			name:    "other index",
			update:  physicalLinkUpdate{InterfaceIndex: 8, InterfaceName: "wlan0", AdminUp: false, LowerUp: false},
			current: &control.Interface{Index: 9, Name: "rmnet_data0"},
		},
		{
			name:    "reused index with another name",
			update:  physicalLinkUpdate{InterfaceIndex: 9, InterfaceName: "wlan0", AdminUp: false, LowerUp: false},
			current: &control.Interface{Index: 9, Name: "rmnet_data0"},
		},
		{
			name:   "no current interface",
			update: physicalLinkUpdate{InterfaceIndex: 9, InterfaceName: "rmnet_data0", Deleted: true},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			observation := observePhysicalLinkUpdate(time.Unix(0, 0), time.Unix(0, 1), testCase.update, testCase.current)
			if observation.CurrentMatch || observation.CandidateLoss {
				t.Fatalf("non-current event classified: %+v", observation)
			}
		})
	}
}

func TestObservePhysicalLinkUpdateDistinguishesHealthyAndDeleted(t *testing.T) {
	current := &control.Interface{Index: 9, Name: "rmnet_data0"}
	healthy := observePhysicalLinkUpdate(time.Time{}, time.Time{}, physicalLinkUpdate{
		InterfaceIndex: 9,
		InterfaceName:  "rmnet_data0",
		AdminUp:        true,
		LowerUp:        true,
	}, current)
	if !healthy.CurrentMatch || healthy.CandidateLoss {
		t.Fatalf("healthy event classified as loss: %+v", healthy)
	}
	deleted := observePhysicalLinkUpdate(time.Time{}, time.Time{}, physicalLinkUpdate{
		InterfaceIndex: 9,
		InterfaceName:  "rmnet_data0",
		AdminUp:        true,
		LowerUp:        true,
		Deleted:        true,
	}, current)
	if !deleted.CandidateLoss {
		t.Fatalf("deleted current link not classified: %+v", deleted)
	}
}

func TestPhysicalLinkObservationLoopDeliversAndClosesIdempotently(t *testing.T) {
	updates := make(chan physicalLinkUpdate, 1)
	emitted := make(chan physicalLinkObservation, 1)
	startedAt := time.Unix(100, 0)
	loop := newPhysicalLinkObservationLoop(
		startedAt,
		func() time.Time { return startedAt.Add(5 * time.Millisecond) },
		func() *control.Interface { return &control.Interface{Index: 9, Name: "rmnet_data0"} },
		func(observation physicalLinkObservation) { emitted <- observation },
	)
	go loop.run(updates)
	updates <- physicalLinkUpdate{InterfaceIndex: 9, InterfaceName: "rmnet_data0", AdminUp: true, LowerUp: false}

	select {
	case observation := <-emitted:
		if !observation.CandidateLoss || observation.MonotonicNanos != int64(5*time.Millisecond) {
			t.Fatalf("unexpected observation: %+v", observation)
		}
	case <-time.After(time.Second):
		t.Fatal("observation was not delivered")
	}

	closed := make(chan struct{})
	go func() {
		loop.close()
		loop.close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("idempotent close blocked")
	}
}

func TestPhysicalLinkObservationLoopPreservesReceiveTimestamp(t *testing.T) {
	updates := make(chan physicalLinkUpdate, 1)
	emitted := make(chan physicalLinkObservation, 1)
	startedAt := time.Unix(100, 0)
	receivedAt := startedAt.Add(3 * time.Millisecond)
	loop := newPhysicalLinkObservationLoop(
		startedAt,
		func() time.Time { return startedAt.Add(time.Second) },
		func() *control.Interface { return &control.Interface{Index: 8, Name: "wlan0"} },
		func(observation physicalLinkObservation) { emitted <- observation },
	)
	go loop.run(updates)
	updates <- physicalLinkUpdate{
		ObservedAt:              receivedAt,
		CurrentCaptured:         true,
		CurrentInterfaceAtEvent: &control.Interface{Index: 9, Name: "rmnet_data0"},
		InterfaceIndex:          9,
		InterfaceName:           "rmnet_data0",
		AdminUp:                 true,
		LowerUp:                 false,
	}
	observation := <-emitted
	if observation.MonotonicNanos != int64(3*time.Millisecond) {
		t.Fatalf("receive timestamp was replaced: %+v", observation)
	}
	if !observation.CurrentMatch || !observation.CandidateLoss {
		t.Fatalf("receive-time current interface was replaced: %+v", observation)
	}
	loop.close()
}

func TestPhysicalLinkObservationLoopStopsWhenSourceCloses(t *testing.T) {
	updates := make(chan physicalLinkUpdate)
	loop := newPhysicalLinkObservationLoop(time.Now(), time.Now, func() *control.Interface { return nil }, func(physicalLinkObservation) {})
	go loop.run(updates)
	close(updates)
	select {
	case <-loop.stopped:
	case <-time.After(time.Second):
		t.Fatal("loop did not stop with source")
	}
	loop.close()
}
