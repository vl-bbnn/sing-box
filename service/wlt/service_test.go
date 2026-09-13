//go:build with_wlt

package wlt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	wltpkg "github.com/sagernet/sing-box/common/wlt"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"
)

func TestNetworkInterfaceIdentityDetectsHandoverAndAddressChange(t *testing.T) {
	lte := &adapter.NetworkInterface{}
	lte.Index = 10
	lte.Name = "cellular0"
	lte.Addresses = []netip.Prefix{netip.MustParsePrefix("192.0.2.2/32")}
	wifi := &adapter.NetworkInterface{}
	wifi.Index = 11
	wifi.Name = "wifi0"
	wifi.Addresses = []netip.Prefix{netip.MustParsePrefix("198.51.100.2/24")}

	lteKey := networkInterfaceKey(lte)
	if interfaceIdentityChanged(lteKey, lteKey) {
		t.Fatal("unchanged interface was classified as a handover")
	}
	if !interfaceIdentityChanged(lteKey, networkInterfaceKey(wifi)) {
		t.Fatal("LTE to Wi-Fi handover was not detected")
	}

	lte.Addresses = []netip.Prefix{netip.MustParsePrefix("192.0.2.3/32")}
	if !interfaceIdentityChanged(lteKey, networkInterfaceKey(lte)) {
		t.Fatal("same-interface address change was not detected")
	}
	if interfaceIdentityChanged("", networkInterfaceKey(lte)) {
		t.Fatal("missing initial identity must not force an unsolicited restart")
	}
}

func TestInterfaceRecoveryUsesClientReloadFallbackDelayOnAndroid(t *testing.T) {
	if got := interfaceRecoveryGraceFor("android"); got != wltAndroidInterfaceFallback {
		t.Fatalf("Android interface recovery delay=%s, want %s", got, wltAndroidInterfaceFallback)
	}
	for _, goos := range []string{"darwin", "ios", "linux"} {
		if got := interfaceRecoveryGraceFor(goos); got != wltInterfaceRecoveryGrace {
			t.Fatalf("%s interface recovery grace=%s, want %s", goos, got, wltInterfaceRecoveryGrace)
		}
	}
}

func TestDuplicateInterfaceUpdatesAreIgnoredOnlyOnAndroid(t *testing.T) {
	const identity = "10|cellular0|cellular|[192.0.2.2/32]"
	if !ignoreDuplicateInterfaceUpdate("android", identity, identity) {
		t.Fatal("duplicate Android callback was not ignored")
	}
	if ignoreDuplicateInterfaceUpdate("ios", identity, identity) {
		t.Fatal("duplicate Apple callback lost its recovery-grace behavior")
	}
	if ignoreDuplicateInterfaceUpdate("android", "", identity) {
		t.Fatal("initial Android callback was ignored")
	}
	if ignoreDuplicateInterfaceUpdate("android", identity, identity+"-new") {
		t.Fatal("real Android handover was ignored")
	}
}

func TestPersistentDNSCacheFileUsesWritableAuthDirectory(t *testing.T) {
	authPath := filepath.Join(t.TempDir(), "wlt-auth.json")
	got := persistentDNSCacheFile(option.WLTServiceOptions{
		AuthSnapshotOutputFile: authPath,
	})
	want := filepath.Join(filepath.Dir(authPath), "wlt-dns-cache.json")
	if got != want {
		t.Fatalf("persistentDNSCacheFile=%q, want %q", got, want)
	}
}

func TestPersistentDNSCacheFileFallsBackToInputAndRejectsRelativePath(t *testing.T) {
	authPath := filepath.Join(t.TempDir(), "wlt-auth.json")
	if got := persistentDNSCacheFile(option.WLTServiceOptions{AuthSnapshotFile: authPath}); got != filepath.Join(filepath.Dir(authPath), "wlt-dns-cache.json") {
		t.Fatalf("persistentDNSCacheFile fallback=%q", got)
	}
	if got := persistentDNSCacheFile(option.WLTServiceOptions{AuthSnapshotOutputFile: "relative.json"}); got != "" {
		t.Fatalf("persistentDNSCacheFile accepted relative path %q", got)
	}
}

func TestPersistentAuthSnapshotAvailableRequiresAbsoluteNonEmptyRegularFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wlt-auth.json")
	options := option.WLTServiceOptions{AuthSnapshotOutputFile: path}
	if persistentAuthSnapshotAvailable(options) {
		t.Fatal("missing snapshot reported as available")
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if persistentAuthSnapshotAvailable(options) {
		t.Fatal("empty snapshot reported as available")
	}
	if err := os.WriteFile(path, []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !persistentAuthSnapshotAvailable(options) {
		t.Fatal("non-empty snapshot was not detected")
	}
	if persistentAuthSnapshotAvailable(option.WLTServiceOptions{AuthSnapshotFile: "relative.json"}) {
		t.Fatal("relative snapshot path reported as available")
	}
}

func TestCarrierRestartRetryDelayBacksOffAndRespectsProviderCooldown(t *testing.T) {
	if got := carrierRestartRetryDelay(1, errors.New("network unavailable")); got != wltCarrierRestartRetryDelay {
		t.Fatalf("first retry delay=%s", got)
	}
	if got := carrierRestartRetryDelay(20, errors.New("network unavailable")); got != wltCarrierRestartRetryMax {
		t.Fatalf("capped retry delay=%s", got)
	}
	if got := carrierRestartRetryDelay(1, carriercommon.ErrHumanChallengeErrorLimit); got != wltCarrierRateLimitRetry {
		t.Fatalf("provider-limited retry delay=%s", got)
	}
}

func TestWaitCarrierWaitsForReplacement(t *testing.T) {
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	service := &Service{
		ctx:          serviceContext,
		carrierReady: make(chan struct{}),
	}
	want := &wltpkg.Carrier{}
	result := make(chan *wltpkg.Carrier, 1)
	errorResult := make(chan error, 1)
	go func() {
		carrier, err := service.WaitCarrier(context.Background())
		result <- carrier
		errorResult <- err
	}()

	select {
	case <-result:
		t.Fatal("WaitCarrier returned before replacement was published")
	case <-time.After(20 * time.Millisecond):
	}

	service.access.Lock()
	service.carrier = want
	close(service.carrierReady)
	service.access.Unlock()

	select {
	case got := <-result:
		if got != want {
			t.Fatalf("WaitCarrier returned %p, want %p", got, want)
		}
		if err := <-errorResult; err != nil {
			t.Fatalf("WaitCarrier returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitCarrier did not observe replacement")
	}
}

func TestWaitCarrierHonorsDialContext(t *testing.T) {
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	service := &Service{
		ctx:          serviceContext,
		carrierReady: make(chan struct{}),
	}
	dialContext, cancelDial := context.WithCancel(context.Background())
	cancelDial()
	if _, err := service.WaitCarrier(dialContext); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitCarrier error = %v, want context cancellation", err)
	}
}

func TestWaitCarrierBlocksDuringInterfaceRecovery(t *testing.T) {
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	service := &Service{
		ctx:            serviceContext,
		carrier:        &wltpkg.Carrier{},
		carrierReady:   closedSignal(),
		interfaceReady: make(chan struct{}),
	}
	result := make(chan *wltpkg.Carrier, 1)
	go func() {
		carrier, _ := service.WaitCarrier(context.Background())
		result <- carrier
	}()
	select {
	case <-result:
		t.Fatal("WaitCarrier returned while interface recovery gate was closed")
	case <-time.After(20 * time.Millisecond):
	}
	service.access.Lock()
	closeSignal(service.interfaceReady)
	service.access.Unlock()
	select {
	case got := <-result:
		if got != service.carrier {
			t.Fatal("WaitCarrier returned the wrong carrier after recovery")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitCarrier did not resume after interface recovery")
	}
}

func TestWaitCarrierRejectsStoppedService(t *testing.T) {
	service := &Service{
		ctx:          context.Background(),
		carrierReady: make(chan struct{}),
		stopped:      true,
	}
	if _, err := service.WaitCarrier(context.Background()); err == nil || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("WaitCarrier error = %v, want stopped service", err)
	}
}

type testTrafficReadyWaiter struct {
	wait func(context.Context) error
}

func (w *testTrafficReadyWaiter) WaitTrafficReady(ctx context.Context) error {
	return w.wait(ctx)
}

type testTrafficReadyState struct {
	access  sync.RWMutex
	carrier trafficReadyWaiter
	stopped bool
	ready   chan struct{}
}

func (s *testTrafficReadyState) snapshot() (trafficReadyWaiter, bool, <-chan struct{}) {
	s.access.RLock()
	defer s.access.RUnlock()
	return s.carrier, s.stopped, s.ready
}

func TestWaitWltTrafficReadyTimesOutBeforeCarrierExists(t *testing.T) {
	service := &Service{ctx: context.Background(), carrierReady: make(chan struct{})}
	err := service.WaitWltTrafficReady(context.Background(), 20)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitWltTrafficReady error = %v, want deadline exceeded", err)
	}
}

func TestWaitWltTrafficReadyStoppedServiceWakesBeforeCarrierExists(t *testing.T) {
	service := &Service{ctx: context.Background(), carrierReady: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		result <- service.WaitWltTrafficReady(context.Background(), 1000)
	}()

	select {
	case err := <-result:
		t.Fatalf("WaitWltTrafficReady returned before stop: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "stopped") {
			t.Fatalf("WaitWltTrafficReady error = %v, want stopped service", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitWltTrafficReady did not wake when the carrier-less service stopped")
	}
}

func TestWaitWltTrafficReadyDoesNotUseRetainedCarrierWhileUnavailable(t *testing.T) {
	service := &Service{
		ctx:                  context.Background(),
		carrier:              &wltpkg.Carrier{},
		carrierReady:         closedSignal(),
		interfaceReady:       make(chan struct{}),
		interfaceUnavailable: true,
	}
	err := service.WaitWltTrafficReady(context.Background(), 20)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitWltTrafficReady error=%v, want deadline while unavailable", err)
	}
}

func TestWaitWltTrafficReadyIgnoresObsoleteCarrierSignal(t *testing.T) {
	oldStarted := make(chan struct{})
	oldReady := make(chan struct{})
	newStarted := make(chan struct{})
	newReady := make(chan struct{})
	oldCarrier := &testTrafficReadyWaiter{wait: func(ctx context.Context) error {
		close(oldStarted)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-oldReady:
			return nil
		}
	}}
	newCarrier := &testTrafficReadyWaiter{wait: func(ctx context.Context) error {
		close(newStarted)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-newReady:
			return nil
		}
	}}
	state := &testTrafficReadyState{carrier: oldCarrier, ready: closedSignal()}
	result := make(chan error, 1)
	go func() {
		result <- waitWltTrafficReady(context.Background(), context.Background(), 1000, state.snapshot)
	}()
	<-oldStarted

	state.access.Lock()
	state.carrier = newCarrier
	state.access.Unlock()
	close(oldReady)
	select {
	case <-newStarted:
	case <-time.After(time.Second):
		t.Fatal("WaitWltTrafficReady did not move to the replacement carrier")
	}
	select {
	case err := <-result:
		t.Fatalf("obsolete carrier readiness satisfied replacement wait: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(newReady)
	if err := <-result; err != nil {
		t.Fatalf("replacement carrier readiness returned error: %v", err)
	}
}

func TestWaitWltTrafficReadyRetriesClosedCarrierAfterRapidReload(t *testing.T) {
	oldStarted := make(chan struct{})
	oldClosed := make(chan struct{})
	newReady := make(chan struct{})
	oldCarrier := &testTrafficReadyWaiter{wait: func(ctx context.Context) error {
		close(oldStarted)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-oldClosed:
			return net.ErrClosed
		}
	}}
	newCarrier := &testTrafficReadyWaiter{wait: func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-newReady:
			return nil
		}
	}}
	state := &testTrafficReadyState{carrier: oldCarrier, ready: closedSignal()}
	result := make(chan error, 1)
	go func() {
		result <- waitWltTrafficReady(context.Background(), context.Background(), 1000, state.snapshot)
	}()
	<-oldStarted

	state.access.Lock()
	state.carrier = newCarrier
	state.access.Unlock()
	close(oldClosed)
	close(newReady)
	if err := <-result; err != nil {
		t.Fatalf("rapid replacement returned old carrier error: %v", err)
	}
}

func TestWaitWltTrafficReadyHonorsCancellation(t *testing.T) {
	waitStarted := make(chan struct{})
	carrier := &testTrafficReadyWaiter{wait: func(ctx context.Context) error {
		close(waitStarted)
		<-ctx.Done()
		return ctx.Err()
	}}
	state := &testTrafficReadyState{carrier: carrier, ready: closedSignal()}
	waitCtx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- waitWltTrafficReady(waitCtx, context.Background(), 1000, state.snapshot)
	}()
	<-waitStarted
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitWltTrafficReady error = %v, want context cancellation", err)
	}
}

func TestInterfaceRecoveryGenerationSupersedesOlderNotification(t *testing.T) {
	service := &Service{interfaceReady: closedSignal()}
	first := service.beginInterfaceRecovery()
	second := service.beginInterfaceRecovery()
	if first == second {
		t.Fatal("interface recovery generation did not advance")
	}
	if service.interfaceRecoveryCurrent(first) {
		t.Fatal("older interface notification remained current")
	}
	if !service.interfaceRecoveryCurrent(second) {
		t.Fatal("latest interface notification was not current")
	}
	service.finishInterfaceRecovery(first)
	select {
	case <-service.interfaceReady:
		t.Fatal("older interface notification opened the current gate")
	default:
	}
	service.finishInterfaceRecovery(second)
	select {
	case <-service.interfaceReady:
	default:
		t.Fatal("latest interface notification did not open the gate")
	}
}

func TestNetworkUnavailableClosesGateAdvancesOnceAndRetainsCarrierStats(t *testing.T) {
	carrier := &wltpkg.Carrier{}
	service := &Service{
		ctx:            context.Background(),
		logger:         log.NewNOPFactory().Logger(),
		carrier:        carrier,
		carrierReady:   closedSignal(),
		interfaceReady: closedSignal(),
		interfaceKey:   "10|cellular0|cellular|[192.0.2.2/32]",
	}
	before := carrier.Stats()
	service.NetworkUnavailable()
	firstGeneration := service.interfaceGeneration
	if firstGeneration != 1 || !service.interfaceUnavailable {
		t.Fatalf("loss state generation=%d unavailable=%t", firstGeneration, service.interfaceUnavailable)
	}
	select {
	case <-service.interfaceReady:
		t.Fatal("network loss left WLT admission open")
	default:
	}
	if service.Carrier() != carrier {
		t.Fatal("network loss discarded the exact carrier and its final counters")
	}
	if after := carrier.Stats(); !reflect.DeepEqual(after, before) {
		t.Fatalf("retained carrier counters changed unexpectedly: before=%+v after=%+v", before, after)
	}

	service.NetworkUnavailable()
	if service.interfaceGeneration != firstGeneration {
		t.Fatalf("repeated loss advanced generation to %d, want %d", service.interfaceGeneration, firstGeneration)
	}
}

func TestSameAndroidInterfaceReturnAfterLossStartsNewRecoveryGeneration(t *testing.T) {
	carrier := &wltpkg.Carrier{}
	service := &Service{
		carrier:              carrier,
		interfaceReady:       make(chan struct{}),
		interfaceGeneration:  7,
		initialized:          true,
		interfaceUnavailable: true,
		interfaceKey:         "10|cellular0|cellular|[192.0.2.2/32]",
	}
	gotCarrier, previous, generation, ignored := service.beginInterfaceUpdate(service.interfaceKey, "android")
	if ignored {
		t.Fatal("same-interface return after loss was suppressed as a duplicate")
	}
	if gotCarrier != carrier || previous != service.interfaceKey {
		t.Fatal("recovery did not retain the exact old interface/carrier identity")
	}
	if generation != 8 || service.interfaceUnavailable {
		t.Fatalf("recovery generation=%d unavailable=%t, want 8/false", generation, service.interfaceUnavailable)
	}
}

func TestRestartCarrierCannotPublishAfterNetworkLoss(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldCarrier := &wltpkg.Carrier{}
	newCarrier := &wltpkg.Carrier{}
	startEntered := make(chan struct{})
	service := &Service{
		ctx:                 ctx,
		logger:              log.NewNOPFactory().Logger(),
		carrier:             oldCarrier,
		carrierReady:        closedSignal(),
		interfaceReady:      make(chan struct{}),
		interfaceGeneration: 3,
		startCarrierHook: func(startCtx context.Context, _ bool) (*wltpkg.Carrier, error) {
			close(startEntered)
			<-startCtx.Done()
			return newCarrier, nil
		},
	}
	done := make(chan struct{})
	go func() {
		service.restartCarrier(oldCarrier, "test handover", 3)
		close(done)
	}()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("restart did not enter carrier start")
	}
	service.NetworkUnavailable()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("network loss did not cancel the in-flight restart")
	}
	if service.Carrier() != nil {
		t.Fatal("canceled restart resurrected a carrier while unavailable")
	}
	if !service.interfaceUnavailable || service.interfaceGeneration != 4 {
		t.Fatalf("post-loss state generation=%d unavailable=%t", service.interfaceGeneration, service.interfaceUnavailable)
	}
}

func TestRestartCarrierDoesNotRetryAfterNetworkLossDuringBackoff(t *testing.T) {
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	oldCarrier := &wltpkg.Carrier{}
	firstAttempt := make(chan struct{})
	var attempts int
	service := &Service{
		ctx:                 serviceContext,
		logger:              log.NewNOPFactory().Logger(),
		carrier:             oldCarrier,
		carrierReady:        closedSignal(),
		interfaceReady:      make(chan struct{}),
		interfaceGeneration: 5,
		startCarrierHook: func(context.Context, bool) (*wltpkg.Carrier, error) {
			attempts++
			if attempts == 1 {
				close(firstAttempt)
			}
			return nil, errors.New("injected unavailable underlay")
		},
	}
	done := make(chan struct{})
	go func() {
		service.restartCarrier(oldCarrier, "test retry backoff", 5)
		close(done)
	}()
	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		t.Fatal("restart did not make its first attempt")
	}
	service.NetworkUnavailable()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("network loss did not cancel restart backoff")
	}
	if attempts != 1 {
		t.Fatalf("restart attempts=%d, want exactly 1 before loss", attempts)
	}
}

func TestRestartCarrierCannotPublishAfterClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oldCarrier := &wltpkg.Carrier{}
	startEntered := make(chan struct{})
	service := &Service{
		ctx:            ctx,
		logger:         log.NewNOPFactory().Logger(),
		carrier:        oldCarrier,
		carrierReady:   closedSignal(),
		interfaceReady: make(chan struct{}),
		startCarrierHook: func(startCtx context.Context, _ bool) (*wltpkg.Carrier, error) {
			close(startEntered)
			<-startCtx.Done()
			return &wltpkg.Carrier{}, nil
		},
	}
	done := make(chan struct{})
	go func() {
		service.restartCarrier(oldCarrier, "test close race", 0)
		close(done)
	}()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("restart did not enter carrier start")
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the in-flight restart")
	}
	if service.Carrier() != nil || !service.stopped {
		t.Fatalf("restart resurrected after Close: carrier=%p stopped=%t", service.Carrier(), service.stopped)
	}
}

func TestInitialStartPublishedContextLivesUntilClose(t *testing.T) {
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	carrier := &wltpkg.Carrier{}
	var carrierContext context.Context
	service := &Service{
		ctx:                  serviceContext,
		logger:               log.NewNOPFactory().Logger(),
		carrierReady:         make(chan struct{}),
		interfaceReady:       closedSignal(),
		markCarrierReadyHook: func(*wltpkg.Carrier) {},
		startCarrierHook: func(startCtx context.Context, _ bool) (*wltpkg.Carrier, error) {
			carrierContext = startCtx
			return carrier, nil
		},
	}
	if err := service.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal(err)
	}
	if carrierContext == nil || carrierContext.Err() != nil {
		t.Fatalf("published initial carrier context is not alive: %v", carrierContext)
	}
	if service.Carrier() != carrier || service.carrierStartCancel == nil {
		t.Fatal("initial carrier did not take ownership of its start context")
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(carrierContext.Err(), context.Canceled) {
		t.Fatalf("Close left initial carrier context alive: %v", carrierContext.Err())
	}
}

func TestRestartPublishedContextLivesUntilNetworkLoss(t *testing.T) {
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	oldCarrier := &wltpkg.Carrier{}
	newCarrier := &wltpkg.Carrier{}
	var carrierContext context.Context
	service := &Service{
		ctx:                  serviceContext,
		logger:               log.NewNOPFactory().Logger(),
		carrier:              oldCarrier,
		carrierReady:         closedSignal(),
		interfaceReady:       closedSignal(),
		interfaceGeneration:  4,
		markCarrierReadyHook: func(*wltpkg.Carrier) {},
		startCarrierHook: func(startCtx context.Context, _ bool) (*wltpkg.Carrier, error) {
			carrierContext = startCtx
			return newCarrier, nil
		},
	}
	service.restartCarrier(oldCarrier, "test successful restart", 4)
	if carrierContext == nil || carrierContext.Err() != nil {
		t.Fatalf("published restart carrier context is not alive: %v", carrierContext)
	}
	if service.Carrier() != newCarrier || service.carrierStartCancel == nil {
		t.Fatal("restart carrier did not take ownership of its start context")
	}
	service.NetworkUnavailable()
	if !errors.Is(carrierContext.Err(), context.Canceled) {
		t.Fatalf("network loss left restart carrier context alive: %v", carrierContext.Err())
	}
	if service.Carrier() != newCarrier {
		t.Fatal("network loss discarded the published carrier counters")
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInitialStartCannotPublishAfterNetworkLoss(t *testing.T) {
	serviceContext, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	startEntered := make(chan struct{})
	service := &Service{
		ctx:                  serviceContext,
		logger:               log.NewNOPFactory().Logger(),
		carrierReady:         make(chan struct{}),
		interfaceReady:       closedSignal(),
		markCarrierReadyHook: func(*wltpkg.Carrier) {},
		startCarrierHook: func(startCtx context.Context, _ bool) (*wltpkg.Carrier, error) {
			close(startEntered)
			<-startCtx.Done()
			return &wltpkg.Carrier{}, nil
		},
	}
	done := make(chan error, 1)
	go func() { done <- service.Start(adapter.StartStateInitialize) }()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("initial start did not enter carrier start")
	}
	service.NetworkUnavailable()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("network loss did not cancel initial carrier start")
	}
	if service.Carrier() != nil {
		t.Fatal("initial start published a carrier while unavailable")
	}
	if !service.interfaceUnavailable || service.interfaceGeneration != 1 {
		t.Fatalf("post-loss state generation=%d unavailable=%t", service.interfaceGeneration, service.interfaceUnavailable)
	}
}

func TestStaleInterfaceRestartNeverStartsCarrier(t *testing.T) {
	oldCarrier := &wltpkg.Carrier{}
	called := false
	service := &Service{
		ctx:                 context.Background(),
		logger:              log.NewNOPFactory().Logger(),
		carrier:             oldCarrier,
		carrierReady:        closedSignal(),
		interfaceReady:      make(chan struct{}),
		interfaceGeneration: 9,
		startCarrierHook: func(context.Context, bool) (*wltpkg.Carrier, error) {
			called = true
			return &wltpkg.Carrier{}, nil
		},
	}
	service.restartCarrier(oldCarrier, "stale timer", 8)
	if called {
		t.Fatal("stale interface generation started a carrier")
	}
	if service.Carrier() != oldCarrier {
		t.Fatal("stale interface generation displaced the current carrier")
	}
}

func TestFormatWLTStatsHeartbeatAcceptsVectorValues(t *testing.T) {
	message := formatWLTStatsHeartbeat(
		"peer_in_bytes_by_index=", []uint64{1, 2},
		" peer_send_queue_by_index=", []int{3, 4},
	)
	if message != "peer_in_bytes_by_index=[1 2] peer_send_queue_by_index=[3 4]" {
		t.Fatalf("unexpected heartbeat: %q", message)
	}
}

func TestDescribeWLTIncident(t *testing.T) {
	previous := wltpkg.CarrierStats{}
	current := wltpkg.CarrierStats{
		FailedStreams:   2,
		RejectedStreams: 1,
	}
	current.Runtime.FullReconnects = 1
	current.Runtime.LastReconnectReason = "tinymux session died"
	current.Runtime.Peer.OnlinePeers = 2
	current.Runtime.Peer.OutgoingQueueFull = 3

	incident := describeWLTIncident(current, previous)
	for _, expected := range []string{
		"reconnects_delta=1",
		"failed_delta=2",
		"rejected_delta=1",
		"peer_out_queue_full_delta=3",
		"last_reconnect=tinymux session died",
	} {
		if !strings.Contains(incident, expected) {
			t.Fatalf("incident %q does not contain %q", incident, expected)
		}
	}
}

func TestDescribeWLTIncidentIgnoresStableCounters(t *testing.T) {
	stats := wltpkg.CarrierStats{
		FailedStreams:   2,
		RejectedStreams: 1,
	}
	stats.Runtime.FullReconnects = 1
	stats.Runtime.Peer.OutgoingQueueFull = 3

	if incident := describeWLTIncident(stats, stats); incident != "" {
		t.Fatalf("expected no incident for stable counters, got %q", incident)
	}
}

func newLifecycleTestService(t *testing.T) *Service {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := &Service{
		ctx: ctx, logger: log.NewNOPFactory().Logger(),
		carrierReady: make(chan struct{}), interfaceReady: closedSignal(),
		markCarrierReadyHook: func(*wltpkg.Carrier) {},
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func awaitLifecycleEvent(t *testing.T, event <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-event:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestInitialStartCloseCancelsAndAbortsLateSuccess(t *testing.T) {
	s := newLifecycleTestService(t)
	entered, aborted, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	lateCarrier := &wltpkg.Carrier{}
	s.startCarrierHook = func(ctx context.Context, _ bool) (*wltpkg.Carrier, error) {
		close(entered)
		<-ctx.Done()
		return lateCarrier, nil
	}
	s.abortCarrierHook = func(c *wltpkg.Carrier) error {
		if c != lateCarrier {
			t.Error("aborted wrong carrier")
		}
		close(aborted)
		return nil
	}
	go func() { _ = s.Start(adapter.StartStateInitialize); close(done) }()
	awaitLifecycleEvent(t, entered, "initial start")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	awaitLifecycleEvent(t, done, "canceled initial start")
	awaitLifecycleEvent(t, aborted, "late carrier abort")
	if s.Carrier() != nil {
		t.Fatal("late start resurrected after Close")
	}
	if err := s.Start(adapter.StartStateInitialize); err == nil {
		t.Fatal("closed service was started again")
	}
}

func TestPublishedCarrierAbortPrecedesParentCancellation(t *testing.T) {
	for _, action := range []string{"loss", "restart"} {
		t.Run(action, func(t *testing.T) {
			s := newLifecycleTestService(t)
			var firstContext context.Context
			first, second := &wltpkg.Carrier{}, &wltpkg.Carrier{}
			calls := 0
			s.startCarrierHook = func(ctx context.Context, _ bool) (*wltpkg.Carrier, error) {
				calls++
				if calls == 1 {
					firstContext = ctx
					return first, nil
				}
				return second, nil
			}
			if err := s.Start(adapter.StartStateInitialize); err != nil {
				t.Fatal(err)
			}
			aborted := false
			s.abortCarrierHook = func(c *wltpkg.Carrier) error {
				if c != first {
					t.Error("aborted wrong carrier")
				}
				if !aborted && firstContext.Err() != nil {
					t.Error("parent canceled before abort could acquire closeOnce")
				}
				aborted = true
				return nil
			}
			if action == "loss" {
				s.NetworkUnavailable()
			} else {
				s.restartCarrierCurrent(first, "lifecycle test")
			}
			if !aborted || !errors.Is(firstContext.Err(), context.Canceled) {
				t.Fatal("old carrier abort/cancel lifecycle incomplete")
			}
		})
	}
}

func TestNetworkReturnStartsCarrierWhenInitialStartWasDeferred(t *testing.T) {
	s := newLifecycleTestService(t)
	started := make(chan struct{})
	var liveContext context.Context
	carrier := &wltpkg.Carrier{}
	s.startCarrierHook = func(ctx context.Context, _ bool) (*wltpkg.Carrier, error) {
		liveContext = ctx
		return carrier, nil
	}
	s.markCarrierReadyHook = func(*wltpkg.Carrier) { close(started) }
	s.NetworkUnavailable()
	if err := s.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal(err)
	}
	if s.Carrier() != nil {
		t.Fatal("initial start published without an underlay")
	}
	s.interfaceUpdated("10|cellular0", "android", 0)
	awaitLifecycleEvent(t, started, "carrier after missing-underlay startup")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := s.WaitCarrier(ctx)
	if err != nil || got != carrier {
		t.Fatalf("returned carrier=%p error=%v", got, err)
	}
	if liveContext.Err() != nil {
		t.Fatal("successful recovery canceled its own context")
	}
}

func TestMissingAndReturnBeforeStartDoesNotLeaveUnavailableLatch(t *testing.T) {
	s := newLifecycleTestService(t)
	calls := 0
	s.startCarrierHook = func(context.Context, bool) (*wltpkg.Carrier, error) {
		calls++
		return &wltpkg.Carrier{}, nil
	}
	s.NetworkUnavailable()
	s.interfaceUpdated("10|cellular0", "android", 0)
	if calls != 0 || s.interfaceUnavailable {
		t.Fatal("pre-initialize return started carrier or left loss latched")
	}
	if err := s.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("initial starts=%d, want 1", calls)
	}
}

func TestInterfaceChangeCancelsInitialOrRestartAttemptAndRejectsLateSuccess(t *testing.T) {
	for _, initial := range []bool{true, false} {
		t.Run(fmt.Sprint("initial=", initial), func(t *testing.T) {
			s := newLifecycleTestService(t)
			s.initialized = true
			entered, aborted, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			late := &wltpkg.Carrier{}
			s.startCarrierHook = func(ctx context.Context, _ bool) (*wltpkg.Carrier, error) {
				close(entered)
				<-ctx.Done()
				return late, nil
			}
			s.abortCarrierHook = func(c *wltpkg.Carrier) error {
				if c == late {
					close(aborted)
				}
				return nil
			}
			go func() {
				if initial {
					_ = s.Start(adapter.StartStateInitialize)
				} else {
					s.restartCarrierCurrent(nil, "generation test")
				}
				close(done)
			}()
			awaitLifecycleEvent(t, entered, "start attempt")
			_, _, generation, ignored := s.beginInterfaceUpdate("11|wifi0", "android")
			if ignored || generation != 1 {
				t.Fatal("interface change did not supersede in-flight start")
			}
			awaitLifecycleEvent(t, done, "superseded attempt")
			awaitLifecycleEvent(t, aborted, "superseded late carrier abort")
			if s.Carrier() != nil {
				t.Fatal("superseded carrier was published")
			}
		})
	}
}

func TestCloseUsesAbortForCarrierAlreadyDeclaredUnavailable(t *testing.T) {
	s := newLifecycleTestService(t)
	s.carrier = &wltpkg.Carrier{}
	s.interfaceUnavailable = true
	called := false
	s.abortCarrierHook = func(*wltpkg.Carrier) error { called = true; return nil }
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("Close chose graceful drain after trusted loss")
	}
}

func TestDuplicateInterfaceDuringStartDoesNotCancelSameGeneration(t *testing.T) {
	s := newLifecycleTestService(t)
	s.initialized = true
	s.interfaceKey = "10|cellular0"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.restartAttempt = &carrierStartAttempt{cancel: cancel, generation: 0}
	_, _, generation, ignored := s.beginInterfaceUpdate(s.interfaceKey, "ios")
	if !ignored || generation != 0 || ctx.Err() != nil {
		t.Fatal("duplicate callback canceled same-interface startup")
	}
}
