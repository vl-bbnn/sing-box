package libbox

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/control"
)

func generationMonitor(refresh func() error) *platformDefaultInterfaceMonitor {
	manager := &lossPriorityManager{
		refresh: refresh,
		finder: &lossPriorityFinder{
			next:   &control.Interface{Index: 7, Name: "cellular-test"},
			events: &[]string{},
		},
	}
	return &platformDefaultInterfaceMonitor{
		platformInterfaceWrapper: &platformInterfaceWrapper{networkManager: manager},
		logger:                   log.NewNOPFactory().Logger(),
	}
}

func waitForGenerationTest(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for interface update")
	}
}

func TestBlockedAvailabilityCannotPublishAfterAuthoritativeLoss(t *testing.T) {
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var refreshes atomic.Int32
	m := generationMonitor(func() error {
		if refreshes.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	var mu sync.Mutex
	var events []string
	m.RegisterCallback(func(i *control.Interface, _ int) {
		mu.Lock()
		defer mu.Unlock()
		if i == nil {
			events = append(events, "loss")
		} else {
			events = append(events, "available")
		}
	})
	go func() { m.updateDefaultInterface("cellular-test", 7, false, false); close(done) }()
	waitForGenerationTest(t, entered)
	// Loss must publish while the prior availability is blocked in refresh.
	// Its own refresh is allowed to block only after the loss callback.
	lossDone := make(chan struct{})
	go func() { m.updateDefaultInterface("", -1, false, false); close(lossDone) }()
	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		gotLoss := len(events) > 0
		mu.Unlock()
		if gotLoss {
			break
		}
		select {
		case <-deadline:
			t.Fatal("loss waited for stale refresh")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(release)
	waitForGenerationTest(t, done)
	waitForGenerationTest(t, lossDone)
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(events, []string{"loss"}) || m.DefaultInterface() != nil {
		t.Fatalf("stale availability published after loss: %v", events)
	}
}

func TestABAReturnRejectsOldAvailability(t *testing.T) {
	entered, release, oldDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var refreshes atomic.Int32
	m := generationMonitor(func() error {
		if refreshes.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})
	var mu sync.Mutex
	var events []string
	m.RegisterCallback(func(i *control.Interface, _ int) {
		mu.Lock()
		defer mu.Unlock()
		if i == nil {
			events = append(events, "loss")
		} else {
			events = append(events, "available")
		}
	})
	go func() { m.updateDefaultInterface("cellular-test", 7, false, false); close(oldDone) }()
	waitForGenerationTest(t, entered)
	m.updateDefaultInterface("", -1, false, false)
	m.updateDefaultInterface("cellular-test", 7, false, false)
	close(release)
	waitForGenerationTest(t, oldDone)
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(events, []string{"loss", "available"}) || m.DefaultInterface() == nil {
		t.Fatalf("old ABA callback won: %v", events)
	}
}

func TestCloseInvalidatesPendingAvailability(t *testing.T) {
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	m := generationMonitor(func() error { close(entered); <-release; return nil })
	var calls atomic.Int32
	m.RegisterCallback(func(*control.Interface, int) { calls.Add(1) })
	go func() { m.updateDefaultInterface("cellular-test", 7, false, false); close(done) }()
	waitForGenerationTest(t, entered)
	m.invalidateDefaultInterfaceUpdates()
	close(release)
	waitForGenerationTest(t, done)
	m.updateDefaultInterface("cellular-test", 7, false, false)
	if calls.Load() != 0 || m.DefaultInterface() != nil {
		t.Fatal("closed monitor published or accepted an interface")
	}
}

func TestUnchangedCurrentInterfaceDoesNotNotifyAgain(t *testing.T) {
	m := generationMonitor(func() error { return nil })
	var calls atomic.Int32
	m.RegisterCallback(func(*control.Interface, int) { calls.Add(1) })
	m.updateDefaultInterface("cellular-test", 7, false, false)
	m.updateDefaultInterface("cellular-test", 7, false, false)
	if calls.Load() != 1 {
		t.Fatalf("unchanged interface notified %d times", calls.Load())
	}
}
