//go:build with_wlt && with_utls

package libbox

import (
	"context"
	boxtls "github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-vmess/vless"
	M "github.com/sagernet/sing/common/metadata"
	"net"
	"testing"
)

func TestProbeUTLSAndVisionReachesScopedAbort(t *testing.T) {
	for _, flow := range []string{"", "xtls-rprx-vision"} {
		t.Run("flow-"+flow, func(t *testing.T) {
			target := &wltProbeAbortConn{wltProbeErrorConn: &wltProbeErrorConn{}}
			sibling := &wltProbeAbortConn{wltProbeErrorConn: &wltProbeErrorConn{}}
			cfg, err := boxtls.NewUTLSClient(context.Background(), nil, "example.com", option.OutboundTLSOptions{Enabled: true, ServerName: "example.com", UTLS: &option.OutboundUTLSOptions{Enabled: true, Fingerprint: "chrome"}})
			if err != nil {
				t.Fatal(err)
			}
			outer, err := cfg.Client(target)
			if err != nil {
				t.Fatal(err)
			}
			client, err := vless.NewClient("00000000-0000-4000-8000-000000000001", flow, nil)
			if err != nil {
				t.Fatal(err)
			}
			leaf, err := client.DialEarlyConn(outer, M.ParseSocksaddr("example.com:443"))
			if err != nil {
				t.Fatal(err)
			}
			if err = closeWLTProbeConn(leaf); err != nil {
				t.Fatal(err)
			}
			if !target.aborted.Load() || target.closed.Load() || sibling.aborted.Load() || sibling.closed.Load() {
				t.Fatalf("production uTLS/Vision wrapper missed scoped abort: target_aborted=%t target_closed=%t", target.aborted.Load(), target.closed.Load())
			}
		})
	}
}

type probeNetConnEdge struct {
	*wltProbeErrorConn
	upstream net.Conn
	self     bool
	calls    int
}

func (c *probeNetConnEdge) NetConn() net.Conn {
	c.calls++
	if c.self {
		return c
	}
	return c.upstream
}
func TestProbeNetConnNilAndCycleKeepBoundedFallback(t *testing.T) {
	for _, self := range []bool{false, true} {
		target := &probeNetConnEdge{wltProbeErrorConn: &wltProbeErrorConn{}, self: self}
		if err := closeWLTProbeConn(target); err != nil {
			t.Fatal(err)
		}
		if !target.closed.Load() || target.calls < 1 || target.calls > 16 {
			t.Fatalf("unbounded or skipped normal fallback: calls=%d closed=%t", target.calls, target.closed.Load())
		}
	}
}
