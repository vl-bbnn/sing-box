//go:build darwin && with_wlt

package libbox

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/control"
)

type handoverTestManager struct {
	adapter.NetworkManager
	refresh func() error
	finder  control.InterfaceFinder
}

func (m *handoverTestManager) UpdateInterfaces() error                  { return m.refresh() }
func (m *handoverTestManager) InterfaceFinder() control.InterfaceFinder { return m.finder }

type handoverTestPlatform struct {
	PlatformInterface
	closeCount int
}

func (p *handoverTestPlatform) CloseDefaultInterfaceMonitor(InterfaceUpdateListener) error {
	p.closeCount++
	return nil
}

type handoverTestFinder struct {
	control.InterfaceFinder
	next   *control.Interface
	err    error
	events *[]string
}

type concurrentHandoverRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *concurrentHandoverRecorder) add(event string) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *concurrentHandoverRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

type concurrentHandoverFinder struct {
	control.InterfaceFinder
	recorder   *concurrentHandoverRecorder
	interfaces map[int]*control.Interface
}

func (f *concurrentHandoverFinder) ByIndex(index int) (*control.Interface, error) {
	iface := f.interfaces[index]
	if iface != nil {
		f.recorder.add("find-" + iface.Name)
	}
	return iface, nil
}

func (f *handoverTestFinder) ByIndex(int) (*control.Interface, error) {
	*f.events = append(*f.events, "find")
	return f.next, f.err
}

type handoverTestLogger struct {
	events      *[]string
	lines       []string
	infoEntered chan struct{}
	infoRelease chan struct{}
}

func (l *handoverTestLogger) record(event string, args ...any) {
	if l.events != nil {
		*l.events = append(*l.events, event)
	}
	l.lines = append(l.lines, fmt.Sprint(args...))
}

func (l *handoverTestLogger) Trace(...any) {}
func (l *handoverTestLogger) Debug(...any) {}
func (l *handoverTestLogger) Info(args ...any) {
	l.record("handover", args...)
	if l.infoEntered != nil {
		close(l.infoEntered)
		<-l.infoRelease
	}
}
func (l *handoverTestLogger) Warn(...any)  {}
func (l *handoverTestLogger) Error(...any) {}
func (l *handoverTestLogger) Fatal(...any) {}
func (l *handoverTestLogger) Panic(...any) {}

func TestDarwinWLTHandoverCancelsBeforeBlockedRefresh(t *testing.T) {
	var events []string
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	newInterface := &control.Interface{Index: 12, Name: "private-new-name"}
	manager := &handoverTestManager{
		refresh: func() error {
			events = append(events, "refresh")
			close(entered)
			<-release
			return nil
		},
		finder: &handoverTestFinder{next: newInterface, events: &events},
	}
	logger := &handoverTestLogger{events: &events}
	monitor := &platformDefaultInterfaceMonitor{
		platformInterfaceWrapper: &platformInterfaceWrapper{
			networkManager:   manager,
			defaultInterface: &control.Interface{Index: 9, Name: "private-old-name"},
		},
		logger: logger,
	}
	cancelContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitor.RegisterCallback(func(iface *control.Interface, _ int) {
		if iface == nil {
			events = append(events, "loss")
			if monitor.DefaultInterface() != nil {
				t.Error("obsolete interface remained visible during loss callback")
			}
			cancel()
			return
		}
		events = append(events, "available")
	})

	go func() {
		monitor.updateDefaultInterface(newInterface.Name, int32(newInterface.Index), false, false)
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("interface refresh was not entered")
	}
	if cancelContext.Err() == nil {
		close(release)
		t.Fatal("handover loss was not published before blocked refresh")
	}
	if want := []string{"loss", "handover", "refresh"}; !reflect.DeepEqual(events, want) {
		close(release)
		t.Fatalf("events before refresh release=%v, want %v", events, want)
	}
	for _, line := range logger.lines {
		if strings.Contains(line, "private-old-name") || strings.Contains(line, "private-new-name") {
			close(release)
			t.Fatalf("diagnostic leaked interface name: %q", line)
		}
	}
	if len(logger.lines) != 1 || !strings.Contains(logger.lines[0], "monotonic_ingress=") || !strings.Contains(logger.lines[0], "callback_to_cancellation_published=") {
		close(release)
		t.Fatalf("missing monotonic handover diagnostics: %v", logger.lines)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handover callback did not finish")
	}
	if want := []string{"loss", "handover", "refresh", "find", "available"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v, want %v", events, want)
	}
	if monitor.DefaultInterface() != newInterface {
		t.Fatal("new interface was not published after refresh")
	}
}

func TestDarwinWLTHandoverCancelsBeforeBlockedLogSink(t *testing.T) {
	logEntered := make(chan struct{})
	logRelease := make(chan struct{})
	losses := make(chan struct{}, 4)
	aDone := make(chan struct{})
	bDone := make(chan struct{})
	next := &control.Interface{Index: 12, Name: "new"}
	manager := &handoverTestManager{
		refresh: func() error {
			return nil
		},
		finder: &handoverTestFinder{next: next},
	}
	logger := &handoverTestLogger{infoEntered: logEntered, infoRelease: logRelease}
	monitor := &platformDefaultInterfaceMonitor{
		platformInterfaceWrapper: &platformInterfaceWrapper{
			networkManager:   manager,
			defaultInterface: &control.Interface{Index: 9, Name: "old"},
		},
		logger: logger,
	}
	cancelContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitor.RegisterCallback(func(iface *control.Interface, _ int) {
		if iface == nil {
			losses <- struct{}{}
			cancel()
		}
	})
	go func() {
		monitor.updateDefaultInterface(next.Name, int32(next.Index), false, false)
		close(aDone)
	}()
	select {
	case <-losses:
	case <-time.After(3 * time.Second):
		t.Fatal("initial handover loss was not published")
	}
	select {
	case <-logEntered:
	case <-time.After(3 * time.Second):
		close(logRelease)
		t.Fatal("handover diagnostic was not entered")
	}
	if cancelContext.Err() == nil {
		close(logRelease)
		t.Fatal("blocked log sink delayed handover cancellation")
	}
	go func() {
		monitor.updateDefaultInterface("", -1, false, false)
		close(bDone)
	}()
	select {
	case <-losses:
	case <-time.After(3 * time.Second):
		close(logRelease)
		t.Fatal("new loss waited for the old blocked log sink")
	}
	select {
	case <-bDone:
	case <-time.After(3 * time.Second):
		close(logRelease)
		t.Fatal("new loss callback did not return while old log sink was blocked")
	}
	if len(logger.lines) != 1 {
		close(logRelease)
		t.Fatalf("unexpected diagnostics while sink blocked: %v", logger.lines)
	}
	close(logRelease)
	select {
	case <-aDone:
	case <-time.After(3 * time.Second):
		t.Fatal("handover did not finish after releasing log sink")
	}
}

func TestDarwinWLTHandoverEpochSuppressesStaleAvailable(t *testing.T) {
	recorder := &concurrentHandoverRecorder{}
	firstRefreshEntered := make(chan struct{})
	firstRefreshRelease := make(chan struct{})
	firstRefresh := atomic.Bool{}
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	oldInterface := &control.Interface{Index: 9, Name: "old"}
	newA := &control.Interface{Index: 12, Name: "new-a"}
	newB := &control.Interface{Index: 13, Name: "new-b"}
	manager := &handoverTestManager{
		refresh: func() error {
			if firstRefresh.CompareAndSwap(false, true) {
				recorder.add("refresh-a")
				close(firstRefreshEntered)
				<-firstRefreshRelease
			} else {
				recorder.add("refresh-b")
			}
			return nil
		},
		finder: &concurrentHandoverFinder{
			recorder:   recorder,
			interfaces: map[int]*control.Interface{newA.Index: newA, newB.Index: newB},
		},
	}
	monitor := &platformDefaultInterfaceMonitor{
		platformInterfaceWrapper: &platformInterfaceWrapper{
			networkManager:   manager,
			defaultInterface: oldInterface,
		},
		logger: log.NewNOPFactory().Logger(),
	}
	monitor.RegisterCallback(func(iface *control.Interface, _ int) {
		if iface == nil {
			recorder.add("loss")
		} else {
			recorder.add("available-" + iface.Name)
		}
	})
	go func() {
		monitor.updateDefaultInterface(newA.Name, int32(newA.Index), false, false)
		close(firstDone)
	}()
	select {
	case <-firstRefreshEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("first refresh was not entered")
	}
	go func() {
		monitor.updateDefaultInterface(newB.Name, int32(newB.Index), false, false)
		close(secondDone)
	}()
	select {
	case <-secondDone:
	case <-time.After(3 * time.Second):
		close(firstRefreshRelease)
		t.Fatal("newer available callback waited for stale refresh")
	}
	if monitor.DefaultInterface() != newB {
		close(firstRefreshRelease)
		t.Fatal("newer interface was not published")
	}
	close(firstRefreshRelease)
	select {
	case <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("stale refresh did not finish")
	}
	for _, event := range recorder.snapshot() {
		if event == "available-new-a" {
			t.Fatalf("stale interface publication survived newer epoch: %v", recorder.snapshot())
		}
	}
}

func TestDarwinWLTHandoverNewLossSupersedesBlockedRefresh(t *testing.T) {
	recorder := &concurrentHandoverRecorder{}
	firstRefreshEntered := make(chan struct{})
	firstRefreshRelease := make(chan struct{})
	firstRefresh := atomic.Bool{}
	losses := make(chan struct{}, 4)
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	manager := &handoverTestManager{
		refresh: func() error {
			if firstRefresh.CompareAndSwap(false, true) {
				close(firstRefreshEntered)
				<-firstRefreshRelease
			}
			return nil
		},
		finder: &concurrentHandoverFinder{recorder: recorder, interfaces: map[int]*control.Interface{12: {Index: 12, Name: "new"}}},
	}
	monitor := &platformDefaultInterfaceMonitor{
		platformInterfaceWrapper: &platformInterfaceWrapper{
			networkManager:   manager,
			defaultInterface: &control.Interface{Index: 9, Name: "old"},
		},
		logger: log.NewNOPFactory().Logger(),
	}
	monitor.RegisterCallback(func(iface *control.Interface, _ int) {
		if iface == nil {
			losses <- struct{}{}
		}
	})
	go func() {
		monitor.updateDefaultInterface("new", 12, false, false)
		close(firstDone)
	}()
	select {
	case <-losses:
	case <-time.After(3 * time.Second):
		t.Fatal("initial loss was not published")
	}
	select {
	case <-firstRefreshEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("first refresh was not entered")
	}
	go func() {
		monitor.updateDefaultInterface("", -1, false, false)
		close(secondDone)
	}()
	select {
	case <-losses:
	case <-time.After(3 * time.Second):
		close(firstRefreshRelease)
		t.Fatal("new loss waited for stale refresh")
	}
	select {
	case <-secondDone:
	case <-time.After(3 * time.Second):
		close(firstRefreshRelease)
		t.Fatal("new loss callback did not return")
	}
	close(firstRefreshRelease)
	select {
	case <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("stale refresh did not finish")
	}
}

func TestDarwinWLTHandoverLossCallbackCanUnregisterBeforeReturn(t *testing.T) {
	var events []string
	next := &control.Interface{Index: 12, Name: "new"}
	manager := &handoverTestManager{
		refresh: func() error { events = append(events, "refresh"); return nil },
		finder:  &handoverTestFinder{next: next, events: &events},
	}
	platform := &handoverTestPlatform{}
	monitor := &platformDefaultInterfaceMonitor{
		platformInterfaceWrapper: &platformInterfaceWrapper{
			iif:              platform,
			networkManager:   manager,
			defaultInterface: &control.Interface{Index: 9, Name: "old"},
		},
		logger: log.NewNOPFactory().Logger(),
	}
	callbackCount := 0
	var unregister func()
	element := monitor.RegisterCallback(func(iface *control.Interface, _ int) {
		callbackCount++
		if iface != nil {
			t.Error("unregistered callback received rapid return edge")
			return
		}
		if monitor.DefaultInterface() != nil {
			t.Error("obsolete interface visible during reentrant callback")
		}
		unregister()
		if err := monitor.Close(); err != nil {
			t.Errorf("reentrant close: %v", err)
		}
	})
	unregister = func() { monitor.UnregisterCallback(element) }
	monitor.updateDefaultInterface(next.Name, int32(next.Index), false, false)
	if callbackCount != 1 {
		t.Fatalf("callback count=%d, want 1", callbackCount)
	}
	if platform.closeCount != 1 {
		t.Fatalf("platform close count=%d, want 1", platform.closeCount)
	}
	if monitor.DefaultInterface() != next {
		t.Fatal("new interface was not published after reentrant unregister")
	}
}

func TestDarwinWLTHandoverSkipsInitialAndDuplicateCallbacks(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		initial *control.Interface
		want    []string
	}{
		{name: "initial", want: []string{"refresh", "find", "available"}},
		{name: "duplicate", initial: &control.Interface{Index: 12, Name: "same"}, want: []string{"refresh", "find"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var events []string
			next := &control.Interface{Index: 12, Name: "same"}
			manager := &handoverTestManager{
				refresh: func() error { events = append(events, "refresh"); return nil },
				finder:  &handoverTestFinder{next: next, events: &events},
			}
			monitor := &platformDefaultInterfaceMonitor{
				platformInterfaceWrapper: &platformInterfaceWrapper{networkManager: manager, defaultInterface: testCase.initial},
				logger:                   log.NewNOPFactory().Logger(),
			}
			monitor.RegisterCallback(func(iface *control.Interface, _ int) {
				if iface == nil {
					events = append(events, "unexpected-loss")
				} else {
					events = append(events, "available")
				}
			})
			monitor.updateDefaultInterface(next.Name, int32(next.Index), false, false)
			if !reflect.DeepEqual(events, testCase.want) {
				t.Fatalf("events=%v, want %v", events, testCase.want)
			}
		})
	}
}

func TestDarwinWLTHandoverRefreshFailureKeepsLossBeforeRecovery(t *testing.T) {
	var events []string
	next := &control.Interface{Index: 12, Name: "new"}
	manager := &handoverTestManager{
		refresh: func() error { events = append(events, "refresh"); return errors.New("injected refresh failure") },
		finder:  &handoverTestFinder{next: next, events: &events},
	}
	monitor := &platformDefaultInterfaceMonitor{
		platformInterfaceWrapper: &platformInterfaceWrapper{
			networkManager:   manager,
			defaultInterface: &control.Interface{Index: 9, Name: "old"},
		},
		logger: log.NewNOPFactory().Logger(),
	}
	monitor.RegisterCallback(func(iface *control.Interface, _ int) {
		if iface == nil {
			events = append(events, "loss")
		} else {
			events = append(events, "available")
		}
	})
	monitor.updateDefaultInterface(next.Name, int32(next.Index), false, false)
	if want := []string{"loss", "refresh", "find", "available"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v, want %v", events, want)
	}
}
