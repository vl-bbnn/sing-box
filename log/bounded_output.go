package log

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
)

const (
	minimumBoundedOutputBytes = 4096
	outputOverflowMarker      = "[WLT-JOURNAL] log_output_overflow schema=1\n"
	terminalOverflowMarker    = "[WLT-JOURNAL] terminal_reserve_overflow schema=1\n"
)

var (
	ErrOutputOverflow         = errors.New("log: output byte quota exceeded")
	ErrTerminalOutputOverflow = errors.New("log: terminal output reserve exceeded")
)

type syncWriteCloser interface {
	io.Writer
	Sync() error
	Close() error
}

// boundedOutputWriter keeps every write record whole. Ordinary records use the
// primary region. Once it fills, a synchronous marker is appended and only the
// small set of required WLT terminal records may consume the reserved tail.
// Close reports every overflow or I/O error even though Log continues feeding
// the independent platform writer.
type boundedOutputWriter struct {
	access           sync.Mutex
	file             syncWriteCloser
	maxBytes         int64
	primaryLimit     int64
	written          int64
	overflow         bool
	terminalOverflow bool
	writeErr         error
	closed           bool
}

func newBoundedOutputWriter(file syncWriteCloser, maxBytes, initialBytes int64) (*boundedOutputWriter, error) {
	if maxBytes < minimumBoundedOutputBytes {
		return nil, errors.New("log: output_max_bytes must be at least 4096")
	}
	if initialBytes < 0 || initialBytes > maxBytes {
		return nil, ErrOutputOverflow
	}
	reserve := maxBytes / 8
	if reserve > 1024*1024 {
		reserve = 1024 * 1024
	}
	minimumReserve := int64(len(outputOverflowMarker) + len(terminalOverflowMarker) + 512)
	if reserve < minimumReserve {
		reserve = minimumReserve
	}
	if reserve >= maxBytes {
		return nil, errors.New("log: output_max_bytes leaves no primary region")
	}
	if initialBytes > maxBytes-reserve {
		return nil, ErrOutputOverflow
	}
	return &boundedOutputWriter{
		file:         file,
		maxBytes:     maxBytes,
		primaryLimit: maxBytes - reserve,
		written:      initialBytes,
	}, nil
}

func (w *boundedOutputWriter) Write(p []byte) (int, error) {
	w.access.Lock()
	defer w.access.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	if w.writeErr != nil || w.terminalOverflow {
		return len(p), nil
	}
	if !w.overflow && int64(len(p)) <= w.primaryLimit-w.written {
		w.writeRecord(p)
		return len(p), nil
	}
	if !w.overflow {
		w.overflow = true
		w.writeRecord([]byte(outputOverflowMarker))
		// Make the fail-closed marker durable before any later terminal record.
		if w.writeErr == nil {
			w.writeErr = w.file.Sync()
		}
	}
	if requiredTerminalRecord(p) && w.writeErr == nil {
		terminalMarkerRoom := int64(len(terminalOverflowMarker))
		if int64(len(p)) <= w.maxBytes-terminalMarkerRoom-w.written {
			w.writeRecord(p)
		} else {
			w.terminalOverflow = true
			w.writeRecord([]byte(terminalOverflowMarker))
			if w.writeErr == nil {
				w.writeErr = w.file.Sync()
			}
		}
	}
	return len(p), nil
}

func (w *boundedOutputWriter) writeRecord(record []byte) {
	if w.writeErr != nil {
		return
	}
	written, err := w.file.Write(record)
	w.written += int64(written)
	if err != nil {
		w.writeErr = err
	} else if written != len(record) {
		w.writeErr = io.ErrShortWrite
	}
}

func (w *boundedOutputWriter) Close() error {
	w.access.Lock()
	defer w.access.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	w.closed = true
	syncErr := w.file.Sync()
	closeErr := w.file.Close()
	var overflowErr, terminalErr error
	if w.overflow {
		overflowErr = ErrOutputOverflow
	}
	if w.terminalOverflow {
		terminalErr = ErrTerminalOutputOverflow
	}
	return errors.Join(w.writeErr, syncErr, closeErr, overflowErr, terminalErr)
}

func requiredTerminalRecord(record []byte) bool {
	for _, marker := range [][]byte{
		[]byte("relay client session stopped"),
		[]byte("relay client peer write coverage incomplete"),
		[]byte("relay client mux terminal stats"),
		[]byte("relay client mux lifetime coverage incomplete"),
		[]byte("relay client mux post-close open rejected"),
		[]byte("relay client peer write terminal stats"),
		[]byte("relay client terminal diagnostics complete"),
		[]byte("relay client terminal diagnostics incomplete"),
	} {
		if bytes.Contains(record, marker) {
			return true
		}
	}
	return false
}
