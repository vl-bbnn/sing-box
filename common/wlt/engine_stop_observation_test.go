//go:build with_wlt

package wlt

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	carrierconfig "github.com/vl-bbnn/wlt-carrier/pkg/config"
	carrierengine "github.com/vl-bbnn/wlt-carrier/pkg/engine"
)

func TestEngineStopObservationKeepsActualNativeError(t *testing.T) {
	c := newTestCarrier(1, 1, 1, time.Second, time.Second)
	c.client = carrierengine.NewClient(carrierconfig.ClientConfig{})
	var lines []string
	c.logf = func(format string, values ...any) { lines = append(lines, fmt.Sprintf(format, values...)) }
	err := c.Close()
	if err == nil || err.Error() != "not running" {
		t.Fatalf("native Stop error changed: %v", err)
	}
	expected := "WLT close diagnostic component=engine_stop error_classes=other returned_error=true"
	if len(lines) != 1 || lines[0] != expected {
		t.Fatalf("missing_engine_stop_diagnostic: %q", lines)
	}
	if again := c.Close(); again != err {
		t.Fatalf("cached original Close error changed: %v / %v", err, again)
	}
	if len(lines) != 1 {
		t.Fatalf("repeated Close called/logged engine Stop again: %q", lines)
	}
}

func TestEngineStopObservationConcurrentCloseHasOneOwner(t *testing.T) {
	c := newTestCarrier(1, 1, 1, time.Second, time.Second)
	c.client = carrierengine.NewClient(carrierconfig.ClientConfig{})
	var mu sync.Mutex
	var lines []string
	c.logf = func(format string, values ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, fmt.Sprintf(format, values...))
	}
	const count = 32
	results := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- c.Close() }()
	}
	wg.Wait()
	close(results)
	var original error
	for err := range results {
		if err == nil || err.Error() != "not running" {
			t.Fatalf("native error lost under concurrent Close: %v", err)
		}
		if original == nil {
			original = err
		} else if original != err {
			t.Fatal("callers did not receive the same actual cached error")
		}
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "component=engine_stop error_classes=other returned_error=true") {
		t.Fatalf("missing_engine_stop_diagnostic_or_multiple_owners: %q", lines)
	}
}

func TestEngineStopObservationDoesNotInventAbsentEngine(t *testing.T) {
	c := newTestCarrier(1, 1, 1, time.Second, time.Second)
	var lines []string
	c.logf = func(format string, values ...any) { lines = append(lines, fmt.Sprintf(format, values...)) }
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if len(lines) != 0 {
		t.Fatalf("engine Stop was not called: %q", lines)
	}
}
