//go:build with_wlt

package option_test

import (
	"context"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"testing"
)

func TestWLTBodyProbeProfileOptInParses(t *testing.T) {
	var opts option.Options
	err := json.UnmarshalContext(include.Context(context.Background()), []byte(`{"outbounds":[{"type":"urltest","tag":"auto","outbounds":["ordinary","wlt"],"wlt_body_probe":{"url":"https://example.com/body","bytes":65536,"unverified_fallback":"wlt"}},{"type":"urltest","tag":"legacy","outbounds":["ordinary","wlt"]}]}`), &opts)
	if err != nil {
		t.Fatal(err)
	}
	body := opts.Outbounds[0].Options.(*option.URLTestOutboundOptions).WLTBodyProbe
	if body == nil || body.Bytes != 65536 || body.UnverifiedFallback != "wlt" {
		t.Fatal("body options not retained")
	}
	if opts.Outbounds[1].Options.(*option.URLTestOutboundOptions).WLTBodyProbe != nil {
		t.Fatal("legacy profile silently opted in")
	}
}
