//go:build with_wlt

package wlt

import carriercommon "github.com/vl-bbnn/wlt-carrier/pkg/common"

type NetworkRecovery = carriercommon.NetworkRecovery

// Nil retains legacy behavior; apps depend only on libbox, never carrier.
func (c *Carrier) SuspendNetworkRecovery() NetworkRecovery {
	if c == nil || c.client == nil {
		return nil
	}
	select {
	case <-c.done:
		return nil
	default:
	}
	return c.client.SuspendNetworkRecovery()
}
