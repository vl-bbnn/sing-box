//go:build with_wlt

package option_test

import (
	"context"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"testing"
)

func TestWLTSessionRetentionProfileCodec(t *testing.T) {
	ctx := include.Context(context.Background())
	for _, tc := range []struct {
		raw     string
		enabled bool
	}{
		{`{"services":[{"type":"wlt","tag":"wlt-carrier","carrier_config":"{}","session_retention_millis":2000}]}`, true},
		{`{"services":[{"type":"wlt","tag":"wlt-carrier","carrier_config":"{}"}]}`, false},
	} {
		var options option.Options
		if e := json.UnmarshalContext(ctx, []byte(tc.raw), &options); e != nil {
			t.Fatal(e)
		}
		if len(options.Services) != 1 {
			t.Fatal("service missing")
		}
		raw, e := json.Marshal(options.Services[0].Options)
		if e != nil {
			t.Fatal(e)
		}
		var fields map[string]any
		if e = json.Unmarshal(raw, &fields); e != nil {
			t.Fatal(e)
		}
		value, present := fields["session_retention_millis"]
		if tc.enabled {
			if !present || value != float64(2000) {
				t.Fatal("requested hold not preserved")
			}
		} else if present {
			t.Fatal("default negotiation changed")
		}
	}
}
