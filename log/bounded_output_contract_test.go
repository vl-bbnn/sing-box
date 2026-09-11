package log

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/option"
)

// This test intentionally compiles against the before source. Reflection makes
// the missing configuration contract a semantic red result instead of a build
// failure.
func TestLogOutputHasHardLiveQuotaContractR3(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.output")
	logOptions := option.LogOptions{Output: path, Timestamp: true}
	quota := reflect.ValueOf(&logOptions).Elem().FieldByName("OutputMaxBytes")
	if !quota.IsValid() || !quota.CanSet() {
		t.Fatal("LogOptions has no output_max_bytes contract")
	}
	quota.SetInt(4096)
	factory, err := New(Options{Context: context.Background(), Options: logOptions})
	if err != nil {
		t.Fatal(err)
	}
	if err := factory.Start(); err != nil {
		t.Fatal(err)
	}
	factory.Logger().Info(strings.Repeat("unbounded-before-record", 500))
	factory.Logger().Info("relay client terminal diagnostics complete terminal_diagnostics_complete=true")
	if err := factory.Close(); err == nil {
		t.Fatal("overflow did not propagate through Factory.Close")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(content) > 4096 {
		t.Fatalf("log.output bytes=%d, maximum=4096", len(content))
	}
	if !strings.Contains(string(content), "[WLT-JOURNAL] log_output_overflow schema=1") {
		t.Fatal("synchronous overflow marker missing")
	}
	if !strings.Contains(string(content), "relay client terminal diagnostics complete") {
		t.Fatal("required terminal record missing after overflow")
	}
}
