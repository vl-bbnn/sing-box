//go:build with_wlt

package wlt

import (
	"context"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"strings"
	"testing"
)

func TestScopedApplicationReadPolicyReachesRealCarrierValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	service := &Service{ctx: ctx, logger: log.NewNOPFactory().Logger(), options: option.WLTServiceOptions{CarrierConfig: "{}", ScopedApplicationReadRetirement: true}}
	carrier, e := service.startCarrier(ctx, false)
	if carrier != nil || e == nil || !strings.Contains(e.Error(), "scoped application read retirement requires data_half_close") {
		t.Fatal("actual service/carrier option forwarding lost", e)
	}
}
