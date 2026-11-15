//go:build with_wlt

package libbox

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWLTRequestHeapProfileWritesCompleteSnapshotOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "heap.pprof")
	if !WLTRequestHeapProfile(path) {
		t.Fatal("first profile request rejected")
	}
	if WLTRequestHeapProfile(path) {
		t.Fatal("duplicate profile request accepted")
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		info, err := os.Stat(path)
		if err == nil {
			if info.Size() == 0 {
				t.Fatal("profile is empty")
			}
			if _, err := os.Stat(path + ".partial"); !os.IsNotExist(err) {
				t.Fatalf("partial profile remains: %v", err)
			}
			return
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("profile was not written")
}
