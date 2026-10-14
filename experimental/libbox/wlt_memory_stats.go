//go:build with_wlt

package libbox

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync/atomic"
)

var wltHeapProfileRequested atomic.Bool

// WLTMemoryStatsJSON exposes aggregate Go allocator metrics to the Dev packet
// tunnel diagnostics. It contains no connection, profile, or user data.
func WLTMemoryStatsJSON() string {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	value := struct {
		HeapAlloc    uint64 `json:"heap_alloc"`
		HeapInuse    uint64 `json:"heap_inuse"`
		HeapIdle     uint64 `json:"heap_idle"`
		HeapReleased uint64 `json:"heap_released"`
		StackSys     uint64 `json:"stack_sys"`
		Sys          uint64 `json:"sys"`
		NumGC        uint32 `json:"num_gc"`
		Goroutines   int    `json:"goroutines"`
	}{
		HeapAlloc:    stats.HeapAlloc,
		HeapInuse:    stats.HeapInuse,
		HeapIdle:     stats.HeapIdle,
		HeapReleased: stats.HeapReleased,
		StackSys:     stats.StackSys,
		Sys:          stats.Sys,
		NumGC:        stats.NumGC,
		Goroutines:   runtime.NumGoroutine(),
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

// WLTRequestHeapProfile writes one Dev diagnostic snapshot per process. The
// caller supplies a path in the app-group cache; profiling runs off the packet
// tunnel's heartbeat queue and never changes its service lifecycle.
func WLTRequestHeapProfile(path string) bool {
	if path == "" || !wltHeapProfileRequested.CompareAndSwap(false, true) {
		return false
	}
	go func() {
		// An exclusive temporary file prevents a partial profile from being
		// mistaken for a usable capture after an extension termination.
		tempPath := path + ".partial"
		file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return
		}
		defer os.Remove(tempPath)
		if err = pprof.WriteHeapProfile(file); err != nil {
			file.Close()
			return
		}
		if err = file.Close(); err != nil {
			return
		}
		if err = os.Rename(tempPath, filepath.Clean(path)); err != nil {
			return
		}
		// Capture the live goroutine stacks beside the heap. A heap sample
		// cannot identify which waiting routines drive StackSys during a
		// PacketTunnel memory-pressure event.
		goroutinePath := strings.TrimSuffix(path, ".pprof") + ".goroutines.pprof"
		goroutineTempPath := goroutinePath + ".partial"
		goroutineFile, err := os.OpenFile(goroutineTempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return
		}
		defer os.Remove(goroutineTempPath)
		if err = pprof.Lookup("goroutine").WriteTo(goroutineFile, 0); err != nil {
			goroutineFile.Close()
			return
		}
		if err = goroutineFile.Close(); err != nil {
			return
		}
		_ = os.Rename(goroutineTempPath, filepath.Clean(goroutinePath))
	}()
	return true
}
