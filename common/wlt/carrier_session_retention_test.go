//go:build with_wlt

package wlt

import (
	"github.com/sagernet/sing/common/json"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	"testing"
)

func TestSessionRetentionOptionSessionLocalAndWireConfig(t *testing.T) {
	original := carrierconfig.ClientConfig{Type: "relay", Routes: []carrierconfig.ClientRoute{{RouteID: "tcp", Socket: "tcp"}}}
	local := original
	if e := applySessionRetentionOption(&local, 2000); e != nil {
		t.Fatal(e)
	}
	if original.SessionRetentionMillis != 0 {
		t.Fatal("source config mutated")
	}
	wire, e := local.ToJSON(true)
	if e != nil {
		t.Fatal(e)
	}
	var decoded carrierconfig.ClientConfig
	if e = json.Unmarshal([]byte(wire), &decoded); e != nil {
		t.Fatal(e)
	}
	if decoded.SessionRetentionMillis != 2000 {
		t.Fatal("stripped primary config lost hold")
	}
	defaultCfg := original
	if e := applySessionRetentionOption(&defaultCfg, 0); e != nil || defaultCfg.SessionRetentionMillis != 0 {
		t.Fatal("legacy default changed")
	}
	embedded := local
	if e := applySessionRetentionOption(&embedded, 0); e != nil || embedded.SessionRetentionMillis != 2000 {
		t.Fatal("absent override deleted configured hold")
	}
	for _, ms := range []int{-1, 1, 999, 1500, 5001, 6000} {
		bad := original
		if e := applySessionRetentionOption(&bad, ms); e == nil || bad.SessionRetentionMillis != 0 {
			t.Fatalf("invalid hold %d admitted/mutated", ms)
		}
	}
	for _, bad := range []carrierconfig.ClientConfig{
		{Type: "direct", Routes: original.Routes},
		{Type: "relay", LowLatencyControl: true, Routes: original.Routes},
		{Type: "relay", Routes: []carrierconfig.ClientRoute{{RouteID: "udp", Socket: "udp"}}},
		{Type: "relay"},
	} {
		if e := applySessionRetentionOption(&bad, 2000); e == nil || bad.SessionRetentionMillis != 0 {
			t.Fatal("unsupported hold admitted/mutated")
		}
	}
	if e := applySessionRetentionOption(nil, 2000); e == nil {
		t.Fatal("nil owner admitted")
	}
}
