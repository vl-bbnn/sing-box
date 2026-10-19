//go:build with_wlt

package group

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	tun "github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

type bodyGroupOutbound struct {
	adapter.Outbound
	tag, address string
	calls        atomic.Int32
	fail         atomic.Bool
}

func (o *bodyGroupOutbound) Type() string           { return "body-test" }
func (o *bodyGroupOutbound) Tag() string            { return o.tag }
func (o *bodyGroupOutbound) Network() []string      { return []string{N.NetworkTCP, N.NetworkUDP} }
func (o *bodyGroupOutbound) Dependencies() []string { return nil }
func (o *bodyGroupOutbound) DialContext(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	o.calls.Add(1)
	if o.fail.Load() {
		return nil, errors.New("body-test route failed")
	}
	return (&net.Dialer{}).DialContext(ctx, network, o.address)
}
func (o *bodyGroupOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	o.calls.Add(1)
	if o.fail.Load() {
		return nil, errors.New("body-test route failed")
	}
	return net.ListenPacket("udp", "127.0.0.1:0")
}
func (o *bodyGroupOutbound) NewDirectRouteConnection(adapter.InboundContext, tun.DirectRouteContext, time.Duration) (tun.DirectRouteDestination, error) {
	o.calls.Add(1)
	return nil, nil
}

type bodyGroupSelector struct {
	*bodyGroupOutbound
	current atomic.Value
}

func (o *bodyGroupSelector) Now() string   { return o.current.Load().(string) }
func (o *bodyGroupSelector) All() []string { return []string{"leaf-a", "leaf-b"} }

type bodyGroupStore struct {
	adapter.CertificateStore
	pool *x509.CertPool
}

func (s *bodyGroupStore) Pool() *x509.CertPool { return s.pool }

func bodyGroupFixture(t *testing.T, ordinary http.HandlerFunc) (*URLTestGroup, *bodyGroupOutbound, *bodyGroupOutbound) {
	t.Helper()
	good := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") }))
	bad := httptest.NewTLSServer(ordinary)
	t.Cleanup(good.Close)
	t.Cleanup(bad.Close)
	a := &bodyGroupOutbound{tag: "ordinary", address: bad.Listener.Addr().String()}
	b := &bodyGroupOutbound{tag: "saved-wlt", address: good.Listener.Addr().String()}
	pool := x509.NewCertPool()
	pool.AddCert(good.Certificate())
	pool.AddCert(bad.Certificate())
	ctx := service.ContextWith[adapter.CertificateStore](context.Background(), &bodyGroupStore{pool: pool})
	manager := &fakeManager{byTag: map[string]adapter.Outbound{a.tag: a, b.tag: b}}
	g, err := NewURLTestGroup(ctx, manager, log.NewNOPFactory().Logger(), []adapter.Outbound{a, b}, good.URL, time.Minute, 50, true, 10*time.Minute, false)
	if err != nil {
		t.Fatal(err)
	}
	err = g.configureWLTBodyProbe(option.WLTURLTestOptions{WLTBodyProbe: &option.WLTBodyProbeOptions{URL: good.URL, Bytes: 4, UnverifiedFallback: b.tag}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	return g, a, b
}
func checkBodyChoice(t *testing.T, g *URLTestGroup, want adapter.Outbound, healthy bool) {
	t.Helper()
	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		got, ok := g.Select(network)
		if got != want || ok != healthy {
			t.Fatalf("network=%s got=%v healthy=%v", network, got, ok)
		}
		if g.selectedOutbound(network) != want {
			t.Fatal("cached route bypasses body policy")
		}
	}
	tcp, udp := g.selectedOutbounds()
	if tcp != want || udp != want {
		t.Fatal("status/direct route snapshot bypasses body policy")
	}
}
func TestWLTBodyProbeHEADHealthyBodyBrokenNeverAdmitsOrdinary(t *testing.T) {
	g, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(200)
			return
		}
		w.Header().Set("Content-Length", "4")
		fmt.Fprint(w, "b")
	})
	g.history.StoreURLTestHistory(a.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 1})
	checkBodyChoice(t, g, b, false)
	if _, err := urltest.URLTest(g.ctx, g.link, a); err != nil {
		t.Fatal("HEAD control", err)
	}
	result, err := g.URLTest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result[a.Tag()]; ok {
		t.Fatal("truncated body stored as healthy")
	}
	checkBodyChoice(t, g, b, true)
	if g.history.LoadURLTestHistory(a.Tag()) == nil {
		t.Fatal("body failure mutated legacy HEAD history")
	}
}
func TestWLTBodyProbeValidOrdinaryAndGroupHistoryIsolation(t *testing.T) {
	g, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	if _, err := g.URLTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkBodyChoice(t, g, a, true)
	g.history.DeleteURLTestHistory(a.Tag())
	checkBodyChoice(t, g, a, true)
	other, err := NewURLTestGroup(g.ctx, g.outbound, g.logger, g.outbounds, g.link, time.Minute, 50, true, 10*time.Minute, false)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err = other.configureWLTBodyProbe(option.WLTURLTestOptions{WLTBodyProbe: &g.wltBodyProbe.options}); err != nil {
		t.Fatal(err)
	}
	other.history = g.history
	g.history.StoreURLTestHistory(a.Tag(), &adapter.URLTestHistory{Time: time.Now(), Delay: 1})
	checkBodyChoice(t, other, b, false)
	// A generation change invalidates strong proof immediately, even though the
	// old cached selection and both unrelated HEAD stores still prefer ordinary.
	g.interfaceUpdateGeneration.Add(1)
	checkBodyChoice(t, g, b, false)
	// Force/GUI/interface calls cannot bypass the per-group traffic admission.
	before := a.calls.Load() + b.calls.Load()
	g.tryURLTest(context.Background(), true)
	if a.calls.Load()+b.calls.Load() != before {
		t.Fatal("forced call exceeded body traffic admission")
	}
}
func TestWLTBodyProbeEveryColdDialPathAndFallbackStaysBounded(t *testing.T) {
	g, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	g.performUpdateCheck()
	o := &URLTest{group: g, logger: log.NewNOPFactory().Logger()}
	dest := M.ParseSocksaddr("example.com:443")
	c, err := o.DialContext(context.Background(), N.NetworkTCP, dest)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	p, err := o.ListenPacket(context.Background(), dest)
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	if _, err = o.NewDirectRouteConnection(adapter.InboundContext{Network: N.NetworkTCP}, nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if o.Now() != b.Tag() || a.calls.Load() != 0 || b.calls.Load() != 3 {
		t.Fatal("cold path used unverified ordinary route")
	}
	b.fail.Store(true)
	if _, err = o.DialContext(context.Background(), N.NetworkTCP, dest); err == nil {
		t.Fatal("failed cold fallback hid failure")
	}
	if a.calls.Load() != 0 {
		t.Fatal("dial alternative admitted unproved ordinary")
	}
	if _, err = o.ListenPacket(context.Background(), dest); err == nil {
		t.Fatal("failed packet fallback hid failure")
	}
	if a.calls.Load() != 0 {
		t.Fatal("packet alternative admitted unproved ordinary")
	}
}
func TestWLTBodyProbeCompletionCannotCrossInterfaceGeneration(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	g, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		<-release
		fmt.Fprint(w, "body")
	})
	done := make(chan struct{})
	go func() { g.URLTest(context.Background()); close(done) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not reach original HTTP handler")
	}
	g.interfaceUpdateGeneration.Add(1)
	close(release)
	<-done
	checkBodyChoice(t, g, b, false)
	g.wltBodyProbe.access.Lock()
	_, ok := g.wltBodyProbe.health[a.Tag()]
	g.wltBodyProbe.access.Unlock()
	if ok {
		t.Fatal("old interface result became fresh proof")
	}
}
func TestWLTBodyProbeSelectorLeafChangeAndNoHistoryFallback(t *testing.T) {
	g, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	selector := &bodyGroupSelector{bodyGroupOutbound: &bodyGroupOutbound{tag: "selector"}}
	selector.current.Store(a.Tag())
	g.outbounds[0] = selector
	if _, err := g.URLTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkBodyChoice(t, g, selector, true)
	selector.current.Store("leaf-b")
	checkBodyChoice(t, g, b, true)
	// A failed selected path may use the explicit WLT fallback; success opening a
	// socket cannot manufacture the missing proof for the changed selector leaf.
	if alt := g.dialAlternative(context.Background(), N.NetworkTCP, selector); alt != b {
		t.Fatal("explicit fallback not selected")
	}
	g.fallbackConnected(N.NetworkTCP, selector)
	checkBodyChoice(t, g, b, true)
}
func TestWLTBodyProbeCloseJoinsOwnedProbeAndRejectsLateSelection(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	g, _, _ := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	done := make(chan struct{})
	go func() { g.URLTest(context.Background()); close(done) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not reach original HTTP handler")
	}
	closed := make(chan struct{})
	go func() { g.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not join cancelled body probe")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe left live after group Close")
	}
	if selected, _ := g.Select(N.NetworkTCP); selected != nil {
		t.Fatal("closed group admitted route")
	}
}
func TestWLTBodyProbeConfigurationBounds(t *testing.T) {
	g, _, _ := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	good := g.wltBodyProbe.options
	for _, mutate := range []func(*option.WLTBodyProbeOptions){func(c *option.WLTBodyProbeOptions) { c.Bytes = 0 }, func(c *option.WLTBodyProbeOptions) { c.Bytes = 65537 }, func(c *option.WLTBodyProbeOptions) { c.URL = "http://example.com/body" }, func(c *option.WLTBodyProbeOptions) { c.URL = "https://name:secret@example.com/" }, func(c *option.WLTBodyProbeOptions) { c.UnverifiedFallback = "missing" }} {
		cfg := good
		mutate(&cfg)
		if err := g.configureWLTBodyProbe(option.WLTURLTestOptions{WLTBodyProbe: &cfg}); err == nil {
			t.Fatal("unsafe body configuration accepted")
		}
	}
	old := g.interval
	g.interval = time.Second
	if err := g.configureWLTBodyProbe(option.WLTURLTestOptions{WLTBodyProbe: &good}); err == nil {
		t.Fatal("unbounded interval accepted")
	}
	g.interval = old
}

func TestWLTBodyProbeCancelledCallerDoesNotPoisonAdmission(t *testing.T) {
	g, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.URLTest(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation lost: %v", err)
	}
	if a.calls.Load()+b.calls.Load() != 0 {
		t.Fatal("cancelled caller reached network")
	}
	if _, err := g.URLTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkBodyChoice(t, g, a, true)
	// Global service cancellation invalidates selection even before lifecycle
	// Close is dispatched. It cannot keep a previously valid ordinary proof.
	g.wltBodyProbe.cancel()
	if got, _ := g.Select(N.NetworkTCP); got != nil {
		t.Fatal("cancelled service still admitted route")
	}
}

func TestWLTBodyProbePublicConstructorWiresOptInAndDefault(t *testing.T) {
	g, _, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	ctx := service.ContextWith[adapter.OutboundManager](g.ctx, g.outbound)
	for _, enabled := range []bool{false, true} {
		cfg := option.URLTestOutboundOptions{Outbounds: []string{"ordinary", b.Tag()}, URL: g.link, Interval: badoption.Duration(time.Minute)}
		if enabled {
			cfg.WLTURLTestOptions = option.WLTURLTestOptions{WLTBodyProbe: &g.wltBodyProbe.options}
		}
		out, err := NewURLTest(ctx, nil, log.NewNOPFactory().Logger(), "constructor", cfg)
		if err != nil {
			t.Fatal(err)
		}
		actual := out.(*URLTest)
		if err = actual.Start(); err != nil {
			t.Fatal(err)
		}
		if (actual.group.wltBodyProbe != nil) != enabled {
			t.Fatal("public URLTest constructor omitted body option")
		}
		actual.Close()
	}
}

func TestWLTBodyProbeAliasesDeduplicateAndInvalidateActualLeaf(t *testing.T) {
	base, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	one := &bodyGroupSelector{bodyGroupOutbound: &bodyGroupOutbound{tag: "one"}}
	one.current.Store(a.Tag())
	two := &bodyGroupSelector{bodyGroupOutbound: &bodyGroupOutbound{tag: "two"}}
	two.current.Store(a.Tag())
	g, err := NewURLTestGroup(base.ctx, base.outbound, base.logger, []adapter.Outbound{one, two, b}, base.link, time.Minute, 50, true, 10*time.Minute, false)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err = g.configureWLTBodyProbe(option.WLTURLTestOptions{WLTBodyProbe: &base.wltBodyProbe.options}); err != nil {
		t.Fatal(err)
	}
	result, err := g.URLTest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a.calls.Load() != 1 || b.calls.Load() != 1 || len(result) != 3 {
		t.Fatalf("aliased leaf was reprobed or proof missing: %v", result)
	}
	checkBodyChoice(t, g, one, true)
	if alt := g.dialAlternative(context.Background(), N.NetworkTCP, one); alt != b {
		t.Fatal("failed real leaf reused through alias")
	}
	checkBodyChoice(t, g, b, true)
}

func TestWLTBodyProbeConcurrencyBudgetAndFairAdmission(t *testing.T) {
	var active, maximum atomic.Int32
	base, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old; old = maximum.Load() {
			if maximum.CompareAndSwap(old, n) {
				break
			}
		}
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	second := &bodyGroupOutbound{tag: "second-stall", address: a.address}
	manager := &fakeManager{byTag: map[string]adapter.Outbound{a.Tag(): a, second.Tag(): second, b.Tag(): b}}
	g, err := NewURLTestGroup(base.ctx, manager, base.logger, []adapter.Outbound{a, second, b}, base.link, time.Minute, 50, true, 10*time.Minute, false)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err = g.configureWLTBodyProbe(option.WLTURLTestOptions{WLTBodyProbe: &base.wltBodyProbe.options}); err != nil {
		t.Fatal(err)
	}
	run := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
		defer cancel()
		start := time.Now()
		if _, err := g.URLTest(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("batch deadline lost: %v", err)
		}
		if time.Since(start) > time.Second {
			t.Fatal("queue restarted total deadline")
		}
	}
	run()
	waitOld := time.Now().Add(time.Second)
	for active.Load() != 0 {
		if time.Now().After(waitOld) {
			t.Fatal("original remote fixture handlers did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	if b.calls.Load() != 0 || maximum.Load() > 2 {
		t.Fatal("concurrency admission exceeded or stall fixture invalid")
	}
	checkBodyChoice(t, g, b, false)
	// Advance only the admission clock in this unit fixture; this represents a
	// later permitted interval. The next batch rotates past the first two stalls.
	g.wltBodyProbe.access.Lock()
	g.wltBodyProbe.lastAttempt = time.Now().Add(-2 * time.Minute)
	g.wltBodyProbe.access.Unlock()
	run()
	if b.calls.Load() != 1 || maximum.Load() > 2 {
		t.Fatal("later healthy member starved or concurrency exceeded")
	}
	checkBodyChoice(t, g, b, true)
}

func TestWLTBodyProbeInterfaceGetsBoundedFreshCheckAndGUICannotFlood(t *testing.T) {
	g, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	if _, err := g.URLTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkBodyChoice(t, g, a, true)
	// A later physical transition after the 5s spacing may borrow one check;
	// both real requests still lie inside the rolling one-minute cap.
	g.wltBodyProbe.access.Lock()
	g.wltBodyProbe.lastAttempt = time.Now().Add(-6 * time.Second)
	g.wltBodyProbe.access.Unlock()
	g.InterfaceUpdated()
	deadline := time.Now().Add(2 * time.Second)
	for {
		selected, healthy := g.Select(N.NetworkTCP)
		if a.calls.Load() == 2 && b.calls.Load() == 2 && selected == a && healthy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("interface did not receive fresh body proof")
		}
		time.Sleep(time.Millisecond)
	}
	g.InterfaceUpdated()
	checkBodyChoice(t, g, b, false)
	g.wltBodyProbe.access.Lock()
	g.wltBodyProbe.lastAttempt = time.Now().Add(-6 * time.Second)
	g.wltBodyProbe.access.Unlock()
	for range 20 {
		g.tryURLTest(context.Background(), true)
	}
	if a.calls.Load() != 2 || b.calls.Load() != 2 {
		t.Fatal("force/GUI/interface bypassed rolling traffic cap")
	}
}

func TestWLTBodyProbeInterfaceCancelsAndJoinsOldNetworkRead(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	g, _, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(entered) })
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	done := make(chan error, 1)
	go func() { _, err := g.URLTest(context.Background()); done <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("old probe did not enter original body reader")
	}
	g.InterfaceUpdated()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("old network cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("old network body reader was not retired")
	}
	checkBodyChoice(t, g, b, false)
}

func TestWLTBodyProbeExpiredProofCannotOutliveConfiguredInterval(t *testing.T) {
	g, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	if _, err := g.URLTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkBodyChoice(t, g, a, true)
	g.wltBodyProbe.access.Lock()
	for tag, proof := range g.wltBodyProbe.health {
		proof.time = time.Now().Add(-2 * time.Minute)
		g.wltBodyProbe.health[tag] = proof
	}
	g.wltBodyProbe.access.Unlock()
	checkBodyChoice(t, g, b, false)
}

func TestWLTBodyProbePublicConstructorRejectsInvalidBeforeStart(t *testing.T) {
	g, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	ctx := service.ContextWith[adapter.OutboundManager](g.ctx, g.outbound)
	for _, name := range []string{"size-zero", "size-large", "http", "credential", "missing-fallback", "too-fast", "too-few", "too-many", "duplicate"} {
		t.Run(name, func(t *testing.T) {
			body := g.wltBodyProbe.options
			cfg := option.URLTestOutboundOptions{Outbounds: []string{a.Tag(), b.Tag()}, URL: g.link, Interval: badoption.Duration(time.Minute), WLTURLTestOptions: option.WLTURLTestOptions{WLTBodyProbe: &body}}
			switch name {
			case "size-zero":
				body.Bytes = 0
			case "size-large":
				body.Bytes = 65537
			case "http":
				body.URL = "http://example.com/body"
			case "credential":
				body.URL = "https://name:secret@example.com/body"
			case "missing-fallback":
				body.UnverifiedFallback = "missing"
			case "too-fast":
				cfg.Interval = badoption.Duration(time.Second)
			case "too-few":
				cfg.Outbounds = []string{b.Tag()}
			case "too-many":
				cfg.Outbounds = []string{a.Tag(), b.Tag(), "c", "d", "e", "f", "g", "h", "i"}
			case "duplicate":
				cfg.Outbounds = []string{a.Tag(), b.Tag(), a.Tag()}
			}
			out, err := NewURLTest(ctx, nil, log.NewNOPFactory().Logger(), "invalid", cfg)
			if err == nil || out != nil {
				t.Fatal("invalid body options survived constructor/check gate")
			}
		})
	}
	if a.calls.Load()+b.calls.Load() != 0 {
		t.Fatal("constructor validation reached network")
	}
}
