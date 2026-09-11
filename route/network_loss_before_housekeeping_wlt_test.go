//go:build with_wlt

package route

import (
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

type lossGateOutbound struct {
	adapter.Outbound
	cancel context.CancelFunc
}

func (*lossGateOutbound) PriorityInterfaceUpdate() {}
func (*lossGateOutbound) InterfaceUpdated()        {}
func (o *lossGateOutbound) NetworkUnavailable()    { o.cancel() }

type blockingLossPause struct {
	pause.Manager
	enter, release chan struct{}
}

func (p *blockingLossPause) NetworkPause() { close(p.enter); <-p.release }

type blockingLossLog struct {
	logger.ContextLogger
	enter, release chan struct{}
}

func (l *blockingLossLog) Error(...any) { close(l.enter); <-l.release }

func TestLossCancelsWLTBeforePauseSubscriberOrLogSinkBlocks(t *testing.T) {
	for _, block := range []string{"pause", "logger"} {
		t.Run(block, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			enter, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			manager := &NetworkManager{logger: log.NewNOPFactory().Logger(), outbound: &staticOutboundManager{outbounds: []adapter.Outbound{&lossGateOutbound{cancel: cancel}}}}
			if block == "pause" {
				manager.pauseManager = &blockingLossPause{enter: enter, release: release}
			} else {
				manager.pauseManager = service.FromContext[pause.Manager](pause.WithDefaultManager(context.Background()))
				manager.logger = &blockingLossLog{ContextLogger: manager.logger, enter: enter, release: release}
			}
			go func() { defer close(done); manager.notifyInterfaceUpdate(nil, 0) }()
			select {
			case <-enter:
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("blocking hook not reached")
			}
			canceledBeforeHousekeeping := ctx.Err() != nil
			close(release)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("loss callback failed to finish")
			}
			if !canceledBeforeHousekeeping {
				t.Fatal("WLT admission was not canceled before blocked housekeeping")
			}
		})
	}
}
