//go:build darwin && cgo

package oomkiller

import (
	"context"
	"reflect"
	"testing"
)

func TestRecoverWLTMemoryPressure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		footprint uint64
		wantReset bool
	}{
		{"reclaimed", 40 << 20, false},
		{"at-limit", 45 << 20, true},
		{"above-limit", 47 << 20, true},
		{"unavailable", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			after, reset := recoverWLTMemoryPressure(
				func() { calls = append(calls, "free") },
				func() uint64 { calls = append(calls, "measure"); return tc.footprint },
				func() { calls = append(calls, "reset") },
			)
			wantCalls := []string{"free", "measure"}
			if tc.wantReset {
				wantCalls = append(wantCalls, "reset", "free")
			}
			if after != tc.footprint || reset != tc.wantReset || !reflect.DeepEqual(calls, wantCalls) {
				t.Fatalf("after=%d reset=%t calls=%v, want %d/%t/%v", after, reset, calls, tc.footprint, tc.wantReset, wantCalls)
			}
		})
	}
}

func TestWLTMemoryPressureOptIn(t *testing.T) {
	if hasWLTMemoryPressure(context.Background()) {
		t.Fatal("ordinary context opted in")
	}
	if !hasWLTMemoryPressure(WithWLTMemoryPressure(context.Background())) {
		t.Fatal("WLT runtime did not opt in")
	}
}
