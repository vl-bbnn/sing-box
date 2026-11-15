//go:build with_wlt

package wlt

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
)

func TestCarrierClientConfigForBuild(t *testing.T) {
	for _, initial := range []bool{false, true} {
		t.Run(map[bool]string{false: "initially_off", true: "initially_on"}[initial], func(t *testing.T) {
			cfg := carrierconfig.ClientConfig{
				UserUUID: "user", Username: "name", PlatformID: "platform", CallID: "call",
				Platform:        json.RawMessage(`{"nested":[1,2]}`),
				Routes:          []carrierconfig.ClientRoute{{RouteID: "r", Socket: "tcp", Transport: "kcp"}},
				GatewayUsername: "gateway-user", Type: "relay", ForceTurn: true, Peers: 3,
				Proto: "proto", Cloak: "cloak", Gateway: "gateway", PubKey: "pubkey",
				Encryption: "full", Name: "name", DiagnosticOpenTrace: initial,
				LowLatencyControl: true,
			}
			before := cfg
			beforeBytes, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			got := carrierClientConfigForBuild(cfg)
			want := before
			want.DiagnosticOpenTrace = initial || forceCarrierOpenTrace
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("unexpected config delta: got %#v, want %#v", got, want)
			}
			if !reflect.DeepEqual(cfg, before) {
				t.Fatal("input config changed")
			}
			afterBytes, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(beforeBytes, afterBytes) {
				t.Fatal("input bytes changed")
			}
		})
	}
}
