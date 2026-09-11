package log

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/option"
)

func newTestBoundedOutput(t *testing.T, maximum int64) (*boundedOutputWriter, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "log.output")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := newBoundedOutputWriter(file, maximum, 0)
	if err != nil {
		t.Fatal(err)
	}
	return writer, path
}

func TestBoundedOutputMarksOverflowAndPreservesTerminalRecords(t *testing.T) {
	writer, path := newTestBoundedOutput(t, 4096)
	prefix := bytes.Repeat([]byte("p"), 3500)
	if _, err := writer.Write(prefix); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(bytes.Repeat([]byte("overflow"), 100)); err != nil {
		t.Fatal(err)
	}
	_, _ = writer.Write([]byte("ordinary record after overflow\n"))
	terminal := []byte("relay client peer write terminal stats peer_write_terminal=true\n")
	if _, err := writer.Write(terminal); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); !errors.Is(err, ErrOutputOverflow) {
		t.Fatalf("Close error=%v, want output overflow", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(content)) > 4096 {
		t.Fatalf("live file bytes=%d, maximum=4096", len(content))
	}
	if bytes.Count(content, []byte(outputOverflowMarker)) != 1 {
		t.Fatalf("overflow marker count=%d", bytes.Count(content, []byte(outputOverflowMarker)))
	}
	if !bytes.Contains(content, terminal) {
		t.Fatal("required terminal record was not preserved after overflow")
	}
	if bytes.Contains(content, []byte("ordinary record after overflow")) {
		t.Fatal("ordinary record consumed the terminal reserve")
	}
}

func TestBoundedOutputReservesEveryTerminalGateRecord(t *testing.T) {
	for _, terminalRecord := range []string{
		"relay client session stopped",
		"relay client peer write coverage incomplete",
		"relay client peer write terminal stats",
		"relay client mux lifetime coverage incomplete",
		"relay client mux terminal stats",
		"relay client mux post-close open rejected",
		"relay client terminal diagnostics complete",
		"relay client terminal diagnostics incomplete",
	} {
		t.Run(terminalRecord, func(t *testing.T) {
			writer, path := newTestBoundedOutput(t, 4096)
			_, _ = writer.Write(bytes.Repeat([]byte("p"), 3500))
			_, _ = writer.Write(bytes.Repeat([]byte("overflow"), 100))
			record := []byte(terminalRecord + " terminal_gate_record=true\n")
			_, _ = writer.Write(record)
			if err := writer.Close(); !errors.Is(err, ErrOutputOverflow) {
				t.Fatalf("Close error=%v, want output overflow", err)
			}
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(content, record) {
				t.Fatalf("reserved record was dropped after primary overflow: %q", terminalRecord)
			}
			if int64(len(content)) > 4096 {
				t.Fatalf("live file bytes=%d, maximum=4096", len(content))
			}
		})
	}
}

func TestBoundedOutputTerminalReserveExhaustionFailsClosed(t *testing.T) {
	writer, path := newTestBoundedOutput(t, 4096)
	_, _ = writer.Write(bytes.Repeat([]byte("p"), 3500))
	_, _ = writer.Write(bytes.Repeat([]byte("overflow"), 100))
	hugeTerminal := append([]byte("relay client mux terminal stats "), bytes.Repeat([]byte("x"), 5000)...)
	_, _ = writer.Write(hugeTerminal)
	err := writer.Close()
	if !errors.Is(err, ErrOutputOverflow) || !errors.Is(err, ErrTerminalOutputOverflow) {
		t.Fatalf("Close error=%v, want primary and terminal overflow", err)
	}
	content, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if int64(len(content)) > 4096 || bytes.Count(content, []byte(terminalOverflowMarker)) != 1 {
		t.Fatalf("invalid terminal overflow evidence bytes=%d marker_count=%d", len(content), bytes.Count(content, []byte(terminalOverflowMarker)))
	}
}

func TestBoundedOutputConcurrentWritesNeverExceedQuota(t *testing.T) {
	writer, path := newTestBoundedOutput(t, 8192)
	record := []byte(strings.Repeat("record-", 20) + "\n")
	var group sync.WaitGroup
	for range 100 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 20 {
				_, _ = writer.Write(record)
			}
		}()
	}
	group.Wait()
	if err := writer.Close(); !errors.Is(err, ErrOutputOverflow) {
		t.Fatalf("Close error=%v, want output overflow", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(content)) > 8192 {
		t.Fatalf("concurrent output bytes=%d, maximum=8192", len(content))
	}
	if bytes.Count(content, []byte(outputOverflowMarker)) != 1 {
		t.Fatal("concurrent overflow marker is not unique")
	}
	prefix := bytes.Split(content, []byte(outputOverflowMarker))[0]
	if len(prefix)%len(record) != 0 {
		t.Fatalf("writer persisted a partial record: prefix=%d record=%d", len(prefix), len(record))
	}
}

type failingBoundedFile struct {
	writeErr error
	syncErr  error
	closeErr error
}

func (f *failingBoundedFile) Write([]byte) (int, error) { return 0, f.writeErr }
func (f *failingBoundedFile) Sync() error               { return f.syncErr }
func (f *failingBoundedFile) Close() error              { return f.closeErr }

type recordingBoundedFile struct {
	bytes.Buffer
	events []string
}

func (f *recordingBoundedFile) Write(p []byte) (int, error) {
	f.events = append(f.events, "write:"+string(p))
	return f.Buffer.Write(p)
}

func (f *recordingBoundedFile) Sync() error {
	f.events = append(f.events, "sync")
	return nil
}

func (f *recordingBoundedFile) Close() error {
	f.events = append(f.events, "close")
	return nil
}

func TestBoundedOutputSyncsOverflowMarkerBeforeTerminalTail(t *testing.T) {
	file := &recordingBoundedFile{}
	writer, err := newBoundedOutputWriter(file, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = writer.Write(bytes.Repeat([]byte("overflow"), 600))
	terminal := "relay client terminal diagnostics incomplete reason=quota\n"
	_, _ = writer.Write([]byte(terminal))
	if len(file.events) < 3 || file.events[0] != "write:"+outputOverflowMarker ||
		file.events[1] != "sync" || file.events[2] != "write:"+terminal {
		t.Fatalf("overflow marker was not synced before terminal tail: %#v", file.events)
	}
	if err := writer.Close(); !errors.Is(err, ErrOutputOverflow) {
		t.Fatalf("Close error=%v, want output overflow", err)
	}
}

func TestBoundedOutputReturnsDeferredWriteSyncAndCloseErrors(t *testing.T) {
	writeErr := errors.New("write failure")
	syncErr := errors.New("sync failure")
	closeErr := errors.New("close failure")
	writer, err := newBoundedOutputWriter(&failingBoundedFile{writeErr, syncErr, closeErr}, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	if written, err := writer.Write([]byte("record\n")); written != 7 || err != nil {
		t.Fatalf("Write=(%d,%v), want accepted deferred failure", written, err)
	}
	err = writer.Close()
	for _, want := range []error{writeErr, syncErr, closeErr} {
		if !errors.Is(err, want) {
			t.Fatalf("Close error=%v missing %v", err, want)
		}
	}
}

func TestBoundedFactoryCloseReturnsDeferredWriteSyncAndCloseErrors(t *testing.T) {
	writeErr := errors.New("factory write failure")
	syncErr := errors.New("factory sync failure")
	closeErr := errors.New("factory close failure")
	writer, err := newBoundedOutputWriter(&failingBoundedFile{writeErr, syncErr, closeErr}, 4096, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = writer.Write([]byte("record\n"))
	factory := NewDefaultFactory(
		context.Background(),
		Formatter{},
		&bytes.Buffer{},
		"",
		nil,
		false,
	).(*defaultFactory)
	factory.boundedFile = writer
	err = factory.Close()
	for _, want := range []error{writeErr, syncErr, closeErr} {
		if !errors.Is(err, want) {
			t.Fatalf("Factory.Close error=%v missing %v", err, want)
		}
	}
}

type boundedOutputPlatformCapture struct {
	access   sync.Mutex
	messages []string
}

func (c *boundedOutputPlatformCapture) WriteMessage(_ Level, message string) {
	c.access.Lock()
	c.messages = append(c.messages, message)
	c.access.Unlock()
}

func TestBoundedFactoryRotationAndPlatformPath(t *testing.T) {
	directory := t.TempDir()
	firstPath := filepath.Join(directory, "generation-0001.log")
	platform := &boundedOutputPlatformCapture{}
	first, err := New(Options{
		Context:        context.Background(),
		Options:        option.LogOptions{Output: firstPath, OutputMaxBytes: 4096, Timestamp: true},
		PlatformWriter: platform,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	first.Logger().Info(strings.Repeat("ordinary", 600))
	first.Logger().Info("relay client terminal diagnostics complete terminal_diagnostics_complete=true")
	if err := first.Close(); !errors.Is(err, ErrOutputOverflow) {
		t.Fatalf("first Close error=%v, want overflow", err)
	}
	platform.access.Lock()
	platformText := strings.Join(platform.messages, "\n")
	platform.access.Unlock()
	if !strings.Contains(platformText, "ordinary") || !strings.Contains(platformText, "terminal diagnostics complete") {
		t.Fatal("bounded file path suppressed the independent platform callback")
	}

	secondPath := filepath.Join(directory, "generation-0002.log")
	second, err := New(Options{
		Context: context.Background(),
		Options: option.LogOptions{Output: secondPath, OutputMaxBytes: 4096},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	second.Logger().Info("new generation")
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(firstPath); err != nil {
		t.Fatal(err)
	}
	if content, err := os.ReadFile(secondPath); err != nil || bytes.Contains(content, []byte(outputOverflowMarker)) {
		t.Fatalf("rotation reused overflow state: content=%q error=%v", content, err)
	}
}

func TestOutputMaxBytesRequiresFileAndRejectsOversizedExistingFile(t *testing.T) {
	if _, err := New(Options{Context: context.Background(), Options: option.LogOptions{Output: "stderr", OutputMaxBytes: 4096}}); err == nil {
		t.Fatal("output_max_bytes accepted stderr")
	}
	path := filepath.Join(t.TempDir(), "existing.log")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 4097), 0o600); err != nil {
		t.Fatal(err)
	}
	factory, err := New(Options{Context: context.Background(), Options: option.LogOptions{Output: path, OutputMaxBytes: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	if err := factory.Start(); !errors.Is(err, ErrOutputOverflow) {
		t.Fatalf("Start error=%v, want overflow", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 4097 {
		t.Fatalf("oversized existing file was mutated: size=%d error=%v", info.Size(), err)
	}
}
