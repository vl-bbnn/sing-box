//go:build with_wlt && !wlt_dial_binding

package wlt

import (
	"context"
	"testing"
)

func TestDialBindingDisabledNoop(t *testing.T) {
	parent := context.Background()
	child, binding := BeginDialBinding(parent, "eu")
	if child != parent || binding.ID != 0 {
		t.Fatal("disabled diagnostic changed context")
	}
	if _, ok := DialBindingFromContext(child); ok {
		t.Fatal("disabled diagnostic reported a binding")
	}
}
