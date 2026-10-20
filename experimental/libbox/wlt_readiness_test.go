//go:build with_wlt

package libbox

import (
	"context"
	"errors"
	"testing"
)

func TestNormalizeWltTrafficReadyTimeout(t *testing.T) {
	err := normalizeWltTrafficReadyError(context.DeadlineExceeded)
	if err == nil || err.Error() != "wlt traffic readiness timed out" {
		t.Fatalf("unexpected timeout error: %v", err)
	}
}

func TestNormalizeWltTrafficReadyPreservesCancellation(t *testing.T) {
	err := normalizeWltTrafficReadyError(context.Canceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation to be preserved, got %v", err)
	}
}
