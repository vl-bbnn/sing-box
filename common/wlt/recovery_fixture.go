//go:build with_wlt && wlt_recovery_fixture

package wlt

import (
	"github.com/vl-bbnn/wlt-carrier/pkg/engine"
	"time"
)

// Test-only adapter initialization; no recovery or channel behavior substituted.
func AdoptRecoveryFixture(client *engine.Client) *Carrier {
	return &Carrier{client: client, done: make(chan struct{}), dialRoute: client.DialRouteContext,
		waitReady: client.WaitReady, startup: newCarrierStartupTelemetry(time.Now(), nil),
		activeSlots: make(chan struct{}, 32), openSlots: make(chan struct{}, 16), pendingSlots: make(chan struct{}, 24),
		routeByClass: map[string]string{"eu": "original-http"}}
}
