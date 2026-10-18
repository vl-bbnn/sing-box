//go:build with_wlt

package group

import (
	"context"
	"fmt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	L "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestWLTBodyReviewSoleColdWrapperCanBeProbedWithoutHEAD(t *testing.T) {
	var heads atomic.Int32
	base, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			heads.Add(1)
		}
		fmt.Fprint(w, "body")
	})
	legacy, err := NewURLTestGroup(base.ctx, base.outbound, base.logger, []adapter.Outbound{a}, base.link, time.Minute, 50, false, 10*time.Minute, false)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	inner := &URLTest{Adapter: outbound.NewAdapter("urltest", "gateway_eu", []string{N.NetworkTCP, N.NetworkUDP}, []string{a.Tag()}), ctx: base.ctx, logger: L.NewNOPFactory().Logger(), tags: []string{a.Tag()}, group: legacy}
	outer := &nestedBodyReview{bodyGroupSelector: &bodyGroupSelector{bodyGroupOutbound: &bodyGroupOutbound{tag: "eu"}}, children: map[string]adapter.Outbound{inner.Tag(): inner}}
	outer.current.Store(inner.Tag())
	manager := base.outbound.(*fakeManager)
	manager.byTag[inner.Tag()] = inner
	manager.byTag[outer.Tag()] = outer
	g, err := NewURLTestGroup(base.ctx, manager, base.logger, []adapter.Outbound{outer, b}, base.link, time.Minute, 50, true, 10*time.Minute, false)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err = g.configureWLTBodyProbe(option.WLTURLTestOptions{WLTBodyProbe: &base.wltBodyProbe.options}); err != nil {
		t.Fatal(err)
	}
	if inner.Now() != "" {
		t.Fatal("fixture expected the actual legacy one-member URL-test before its first HEAD publication")
	}
	if _, err = g.URLTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.calls.Load() != 1 || heads.Load() != 0 {
		t.Fatal("strong GET was blocked by missing weak HEAD history on the single fixed child")
	}
	if selected, healthy := g.Select(N.NetworkTCP); selected != outer || !healthy {
		t.Fatal("completed ordinary body could not be selected")
	}
	client := &URLTest{group: g, logger: L.NewNOPFactory().Logger()}
	conn, err := client.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.com:443"))
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if a.calls.Load() != 2 || inner.Now() != "" {
		t.Fatal("public Dial bypassed the fixed wrapper or fabricated its weak history")
	}
}

// An actual cold legacy URLTest with multiple routes has no selected leaf.
// The body probe must neither guess its first member nor fabricate HEAD state.
func TestWLTBodyReviewColdMultipleWrapperKeepsExplicitFallback(t *testing.T) {
	base, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	other := &bodyGroupOutbound{tag: "other", address: a.address}
	manager := base.outbound.(*fakeManager)
	manager.byTag[other.Tag()] = other
	legacy, err := NewURLTestGroup(base.ctx, manager, base.logger, []adapter.Outbound{a, other}, base.link, time.Minute, 50, false, 10*time.Minute, false)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	inner := &URLTest{Adapter: outbound.NewAdapter("urltest", "cold_multi", []string{N.NetworkTCP, N.NetworkUDP}, []string{a.Tag(), other.Tag()}), ctx: base.ctx, logger: L.NewNOPFactory().Logger(), tags: []string{a.Tag(), other.Tag()}, group: legacy}
	manager.byTag[inner.Tag()] = inner
	g, err := NewURLTestGroup(base.ctx, manager, base.logger, []adapter.Outbound{inner, b}, base.link, time.Minute, 50, true, 10*time.Minute, false)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err = g.configureWLTBodyProbe(option.WLTURLTestOptions{WLTBodyProbe: &base.wltBodyProbe.options}); err != nil {
		t.Fatal(err)
	}
	if _, err = g.URLTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.calls.Load() != 0 || other.calls.Load() != 0 || inner.Now() != "" {
		t.Fatal("cold multi-member wrapper guessed an unresolved route")
	}
	if selected, _ := g.Select(N.NetworkTCP); selected != b {
		t.Fatal("cold unresolved wrapper escaped explicit fallback")
	}
}
