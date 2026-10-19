//go:build with_wlt

package wlt

import (
	"context"
	"crypto/mlkem"
	"encoding/base64"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	"strings"
	"testing"
)

func TestSessionRetentionJSONReachesRealCarrierStartupValidation(t *testing.T) {
	key, e := mlkem.GenerateKey768()
	if e != nil {
		t.Fatal(e)
	}
	cfg := carrierconfig.ClientConfig{Type: "relay", Encryption: "handshake", UserUUID: "00000000-0000-4000-8000-000000000001", Username: "fixture", CallID: "fixture", PlatformID: "vk.com", Peers: 1, PubKey: base64.StdEncoding.EncodeToString(key.EncapsulationKey().Bytes()), Routes: []carrierconfig.ClientRoute{{RouteID: "tcp", Socket: "tcp", Transport: "kcp"}}}
	if e = cfg.Validate(); e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(cfg)
	if e != nil {
		t.Fatal(e)
	}
	var opts option.WLTServiceOptions
	rawOptions, e := json.Marshal(map[string]any{"carrier_config": string(raw), "session_retention_millis": 6000})
	if e != nil {
		t.Fatal(e)
	}
	if e = opts.UnmarshalJSONContext(context.Background(), rawOptions); e != nil {
		t.Fatal(e)
	}
	// A canceled context guarantees this negative startup cannot contact a
	// provider even if a future regression drops the new option forwarding.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &Service{ctx: ctx, logger: log.NewNOPFactory().Logger(), options: opts}
	carrier, e := s.startCarrier(ctx, false)
	if carrier != nil || e == nil || !strings.Contains(e.Error(), "session retention requires an integer") {
		t.Fatalf("actual service/carrier path lost validation: carrier=%v err=%v", carrier, e)
	}
}
