//go:build with_wlt

package group

import (
	"context"
	"fmt"
	"github.com/sagernet/sing-box/adapter"
	L "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"net"
	"net/http"
	"testing"
	"time"
)

type nestedBodyReview struct {
	*bodyGroupSelector
	children map[string]adapter.Outbound
}

func (o *nestedBodyReview) DialContext(ctx context.Context, n string, d M.Socksaddr) (net.Conn, error) {
	return o.children[o.Now()].DialContext(ctx, n, d)
}
func TestWLTBodyReviewNestedLeafInvalidatesProof(t *testing.T) {
	base, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	bad := &bodyGroupOutbound{tag: "other-unproved-leaf", address: a.address}
	bad.fail.Store(true)
	inner := &nestedBodyReview{bodyGroupSelector: &bodyGroupSelector{bodyGroupOutbound: &bodyGroupOutbound{tag: "nested-group"}}, children: map[string]adapter.Outbound{a.Tag(): a, bad.Tag(): bad}}
	inner.current.Store(a.Tag())
	outer := &nestedBodyReview{bodyGroupSelector: &bodyGroupSelector{bodyGroupOutbound: &bodyGroupOutbound{tag: "outer-selector"}}, children: map[string]adapter.Outbound{inner.Tag(): inner}}
	outer.current.Store(inner.Tag())
	manager := base.outbound.(*fakeManager)
	manager.byTag[inner.Tag()] = inner
	manager.byTag[a.Tag()] = a
	manager.byTag[bad.Tag()] = bad
	manager.byTag[outer.Tag()] = outer
	g, err := NewURLTestGroup(base.ctx, manager, base.logger, []adapter.Outbound{outer, b}, base.link, time.Minute, 50, true, 10*time.Minute, false)
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
	if selected, healthy := g.Select(N.NetworkTCP); selected != outer || !healthy {
		t.Fatal("fixture original nested path never proved its full body")
	}
	inner.current.Store("other-unproved-leaf")
	client := &URLTest{group: g, logger: L.NewNOPFactory().Logger()}
	conn, err := client.DialContext(context.Background(), N.NetworkTCP, M.ParseSocksaddr("example.com:443"))
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if bad.calls.Load() != 0 {
		t.Fatal("actual public Dial attempted the new unproved nested leaf before falling back")
	}
	if selected, _ := g.Select(N.NetworkTCP); selected != b {
		t.Fatal("nested leaf change reused old body proof")
	}
}
func TestWLTBodyReviewNestedUnresolvedChoicesUseExplicitFallback(t *testing.T) {
	base, a, b := bodyGroupFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "body") })
	inner := &nestedBodyReview{bodyGroupSelector: &bodyGroupSelector{bodyGroupOutbound: &bodyGroupOutbound{tag: "nested"}}, children: map[string]adapter.Outbound{a.Tag(): a}}
	inner.current.Store(a.Tag())
	outer := &nestedBodyReview{bodyGroupSelector: &bodyGroupSelector{bodyGroupOutbound: &bodyGroupOutbound{tag: "outer"}}, children: map[string]adapter.Outbound{inner.Tag(): inner}}
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
	if _, err = g.URLTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, choice := range []string{"", "missing-member", outer.Tag()} {
		inner.current.Store(choice)
		if selected, _ := g.Select(N.NetworkTCP); selected != b {
			t.Fatal("unknown or cyclic nested choice reused old body proof")
		}
	}
}
