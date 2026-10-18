//go:build !with_wlt

package option_test

import (
	"context"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"strings"
	"testing"
)

func TestOrdinaryBuildRejectsWLTBodyProbeProfile(t *testing.T) {
	var opts option.Options
	ctx := include.Context(context.Background())
	err := json.UnmarshalContext(ctx, []byte(`{"outbounds":[{"type":"urltest","tag":"auto","outbounds":["ordinary","wlt"],"wlt_body_probe":{"url":"https://example.com/body","bytes":65536,"unverified_fallback":"wlt"}}]}`), &opts)
	if err == nil || !strings.Contains(err.Error(), "wlt_body_probe") {
		t.Fatalf("ordinary build accepted WLT-only option: %v", err)
	}
	if err = json.UnmarshalContext(ctx, []byte(`{"outbounds":[{"type":"urltest","tag":"legacy","outbounds":["ordinary","fallback"]}]}`), &opts); err != nil {
		t.Fatal("legacy profile rejected", err)
	}
}
