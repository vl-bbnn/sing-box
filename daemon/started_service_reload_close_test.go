package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	S "github.com/sagernet/sing/service"
)

func TestReloadPropagatesOldInstanceCloseError(t *testing.T) {
	service := NewStartedService(ServiceOptions{Context: S.ContextWithDefaultRegistry(context.Background())})
	defer service.Close()
	profileContent := fmt.Sprintf(`{"experimental":{"cache_file":{"enabled":true,"path":%q}}}`, filepath.Join(t.TempDir(), "cache.db"))
	oldInstance, err := service.newInstance(profileContent, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := oldInstance.Close(); err != nil {
		t.Fatalf("initial close: %v", err)
	}
	service.serviceAccess.Lock()
	service.instance = oldInstance
	service.updateStatus(ServiceStatus_STARTED)
	service.serviceAccess.Unlock()

	err = service.StartOrReloadService(profileContent, nil)
	if err == nil {
		t.Fatal("reload discarded the old instance close error")
	}
	service.serviceAccess.RLock()
	defer service.serviceAccess.RUnlock()
	if service.serviceStatus.Status != ServiceStatus_FATAL {
		t.Fatalf("status=%s, want FATAL", service.serviceStatus.Status)
	}
	if service.instance != nil {
		t.Fatal("reload retained or launched an instance after old close failed")
	}
}
