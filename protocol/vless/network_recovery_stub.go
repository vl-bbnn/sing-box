//go:build !with_wlt

package vless

func (*Outbound) preserveWLTNetworkReset() bool { return false }
