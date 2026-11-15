//go:build with_wlt

package wlt

import (
	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	"testing"
)

func TestDataHalfCloseOptionIsExplicitAndSessionLocal(t *testing.T) {
	original := carrierconfig.ClientConfig{}
	local := original
	if e := applyDataHalfCloseOption(&local, false); e != nil || local.DataHalfClose {
		t.Fatal("default changed")
	}
	if e := applyDataHalfCloseOption(&local, true); e != nil || !local.DataHalfClose {
		t.Fatal("explicit feature missing")
	}
	if original.DataHalfClose {
		t.Fatal("source config mutated")
	}
	incompatible := carrierconfig.ClientConfig{LowLatencyControl: true}
	if e := applyDataHalfCloseOption(&incompatible, true); e == nil || incompatible.DataHalfClose {
		t.Fatal("unordered feature admitted")
	}
	if e := applyDataHalfCloseOption(&incompatible, false); e != nil {
		t.Fatal("legacy unordered path changed")
	}
	if e := applyDataHalfCloseOption(nil, true); e == nil {
		t.Fatal("nil config accepted")
	}
}
