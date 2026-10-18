//go:build !with_wlt

package group

import (
	"context"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"time"
)

type wltBodyProbe struct{}

func (g *URLTestGroup) configureWLTBodyProbe(option.WLTURLTestOptions) error { return nil }
func (g *URLTestGroup) closeWLTBodyProbe()                                   {}
func (g *URLTestGroup) selectWLTBodyProbe(string, adapter.Outbound) (adapter.Outbound, bool) {
	return nil, false
}
func (g *URLTestGroup) alternativeWLTBodyProbe(string, adapter.Outbound) adapter.Outbound { return nil }
func (g *URLTestGroup) tryWLTBodyProbe(context.Context, bool) (map[string]uint16, error, bool) {
	panic("WLT body probe absent from this build")
}

func (g *URLTestGroup) invalidateWLTBodyProbeInterface() {}

func validateWLTBodyProbeOptions(option.WLTURLTestOptions, []string, time.Duration) error { return nil }
