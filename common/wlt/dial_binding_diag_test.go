//go:build with_wlt && wlt_dial_binding

package wlt

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"
)

type bindingParentKey struct{}

func TestDialBindingUniqueConcurrent(t *testing.T) {
	nextDialBindingID.Store(0)
	const count = 512
	results := make(chan uint64, count)
	var jobs sync.WaitGroup
	for i := 0; i < count; i++ {
		jobs.Add(1)
		go func() {
			defer jobs.Done()
			ctx, binding := BeginDialBinding(context.Background(), "eu")
			got, ok := DialBindingFromContext(ctx)
			if !ok || got != binding || binding.EntryUnixNS <= 0 || binding.EntryRoute != "eu" {
				results <- 0
				return
			}
			results <- binding.ID
		}()
	}
	jobs.Wait()
	close(results)
	seen := map[uint64]bool{}
	for id := range results {
		if id == 0 || seen[id] {
			t.Fatalf("duplicate or absent local dial ID: %d", id)
		}
		seen[id] = true
	}
	if len(seen) != count {
		t.Fatalf("got %d local IDs, want %d", len(seen), count)
	}
}

func TestDialBindingNestedContextAndDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), bindingParentKey{}, "retained"), time.Minute)
	defer cancel()
	wantDeadline, _ := parent.Deadline()
	child, binding := BeginDialBinding(parent, "ru")
	if got := child.Value(bindingParentKey{}); got != "retained" {
		t.Fatalf("parent context value lost: %v", got)
	}
	if deadline, ok := child.Deadline(); !ok || !deadline.Equal(wantDeadline) {
		t.Fatal("deadline changed")
	}
	if got, ok := DialBindingFromContext(context.WithValue(child, bindingParentKey{}, "nested")); !ok || got != binding {
		t.Fatal("nested context lost dial binding")
	}
	if _, ok := DialBindingFromContext(parent); ok {
		t.Fatal("parent context was mutated")
	}
	nested, next := BeginDialBinding(child, "eu")
	if next.ID == binding.ID || next.EntryRoute != "eu" {
		t.Fatal("nested dial reused local ID or route")
	}
	if got, ok := DialBindingFromContext(nested); !ok || got != next {
		t.Fatal("nested dial lost its binding")
	}
	if got, ok := DialBindingFromContext(child); !ok || got != binding {
		t.Fatal("nested dial mutated outer binding")
	}
}

func TestDialBindingNoDeadlineAndCancellation(t *testing.T) {
	parent, cancel := context.WithCancelCause(context.Background())
	child, binding := BeginDialBinding(parent, "eu")
	if _, ok := child.Deadline(); ok {
		t.Fatal("diagnostic introduced a deadline")
	}
	if got, ok := DialBindingFromContext(child); !ok || got != binding {
		t.Fatal("binding missing before cancellation")
	}
	cancel(context.Canceled)
	select {
	case <-child.Done():
	case <-time.After(time.Second):
		t.Fatal("parent cancellation did not reach child")
	}
}

func TestDialBindingCounterSaturates(t *testing.T) {
	nextDialBindingID.Store(math.MaxUint64 - 1)
	t.Cleanup(func() { nextDialBindingID.Store(0) })
	_, last := BeginDialBinding(context.Background(), "eu")
	if last.ID != math.MaxUint64 {
		t.Fatalf("last ID=%d", last.ID)
	}
	parent := context.Background()
	ctx, exhausted := BeginDialBinding(parent, "eu")
	if ctx != parent || exhausted.ID != 0 || nextDialBindingID.Load() != math.MaxUint64 {
		t.Fatal("counter wrapped or context changed at cap")
	}
}
