package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	boxlog "github.com/sagernet/sing-box/log"
	S "github.com/sagernet/sing/service"
)

func boundedDaemonProfile(logPath, cachePath string) string {
	return fmt.Sprintf(
		`{"log":{"output":%q,"output_max_bytes":4096,"timestamp":true},"experimental":{"cache_file":{"enabled":true,"path":%q}}}`,
		logPath,
		cachePath,
	)
}

func TestReloadStopsOnBoundedLogOverflowR3(t *testing.T) {
	directory := t.TempDir()
	firstLog := filepath.Join(directory, "generation-0001.log")
	secondLog := filepath.Join(directory, "generation-0002.log")
	cachePath := filepath.Join(directory, "cache.db")
	service := NewStartedService(ServiceOptions{Context: S.ContextWithDefaultRegistry(context.Background())})
	defer service.Close()

	if err := service.StartOrReloadService(boundedDaemonProfile(firstLog, cachePath), nil); err != nil {
		t.Fatal(err)
	}
	service.serviceAccess.RLock()
	oldInstance := service.instance
	service.serviceAccess.RUnlock()
	oldInstance.instance.LogFactory().Logger().Info(strings.Repeat("ordinary-overflow", 600))
	oldInstance.instance.LogFactory().Logger().Info(
		"relay client terminal diagnostics complete terminal_diagnostics_complete=true",
	)

	err := service.StartOrReloadService(boundedDaemonProfile(secondLog, cachePath), nil)
	if !errors.Is(err, boxlog.ErrOutputOverflow) {
		t.Fatalf("reload error=%v, want log output overflow", err)
	}
	service.serviceAccess.RLock()
	if service.serviceStatus.Status != ServiceStatus_FATAL || service.instance != nil {
		service.serviceAccess.RUnlock()
		t.Fatalf("overflow reload state=%s instance=%p, want FATAL/nil", service.serviceStatus.Status, service.instance)
	}
	service.serviceAccess.RUnlock()
	content, readErr := os.ReadFile(firstLog)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(content) > 4096 {
		t.Fatalf("generation bytes=%d, maximum=4096", len(content))
	}
	for _, required := range []string{
		"[WLT-JOURNAL] log_output_overflow schema=1",
		"relay client terminal diagnostics complete",
	} {
		if !strings.Contains(string(content), required) {
			t.Fatalf("missing %q in bounded generation", required)
		}
	}
	if _, statErr := os.Stat(secondLog); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("reload constructed replacement log file: %v", statErr)
	}
}
