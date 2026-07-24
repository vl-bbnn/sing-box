package v2rayxhttp

import (
	"io"
	"net"
	"os"
	"sync"
	"time"
)

type uploadQueue struct {
	access      sync.Mutex
	updated     *sync.Cond
	packets     map[uint64][]byte
	expectedSeq uint64
	current     []byte
	maxBuffered int
	closed      bool
}

func newUploadQueue(maxBuffered int) *uploadQueue {
	queue := &uploadQueue{
		packets:     make(map[uint64][]byte),
		maxBuffered: maxBuffered,
	}
	queue.updated = sync.NewCond(&queue.access)
	return queue
}

func (q *uploadQueue) Push(seq uint64, payload []byte) error {
	q.access.Lock()
	defer q.access.Unlock()
	if q.closed {
		return net.ErrClosed
	}
	if seq < q.expectedSeq {
		return errUploadSequenceConsumed
	}
	if _, loaded := q.packets[seq]; loaded {
		return errUploadSequenceDuplicate
	}
	// Always accept the next expected packet. HTTP/2 request streams can finish
	// out of order; later packets may fill the reorder buffer while this packet
	// is still in flight. Rejecting it would leave an unfillable sequence gap
	// and permanently stall the byte stream. The temporary +1 entry remains
	// bounded and is consumed as soon as the waiting reader acquires the lock.
	if seq != q.expectedSeq && len(q.packets) >= q.maxBuffered {
		return errUploadQueueFull
	}
	q.packets[seq] = payload
	q.updated.Broadcast()
	return nil
}

func (q *uploadQueue) Read(p []byte) (int, error) {
	q.access.Lock()
	defer q.access.Unlock()
	for {
		if len(q.current) > 0 {
			n := copy(p, q.current)
			q.current = q.current[n:]
			return n, nil
		}
		if payload, loaded := q.packets[q.expectedSeq]; loaded {
			delete(q.packets, q.expectedSeq)
			q.expectedSeq++
			q.current = payload
			continue
		}
		if q.closed {
			return 0, io.EOF
		}
		q.updated.Wait()
	}
}

func (q *uploadQueue) Close() error {
	q.access.Lock()
	defer q.access.Unlock()
	if !q.closed {
		q.closed = true
		q.updated.Broadcast()
	}
	return nil
}

type serverPacketConn struct {
	reader     io.ReadCloser
	writer     io.Writer
	flusher    interface{ Flush() }
	localAddr  net.Addr
	remoteAddr net.Addr
	writeMu    sync.Mutex
	closeOnce  sync.Once
}

func (c *serverPacketConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *serverPacketConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	n, err := c.writer.Write(p)
	if err == nil {
		c.flusher.Flush()
	}
	return n, err
}

func (c *serverPacketConn) Close() error {
	c.closeOnce.Do(func() {
		_ = c.reader.Close()
	})
	return nil
}

func (c *serverPacketConn) LocalAddr() net.Addr              { return c.localAddr }
func (c *serverPacketConn) RemoteAddr() net.Addr             { return c.remoteAddr }
func (c *serverPacketConn) SetDeadline(time.Time) error      { return os.ErrInvalid }
func (c *serverPacketConn) SetReadDeadline(time.Time) error  { return os.ErrInvalid }
func (c *serverPacketConn) SetWriteDeadline(time.Time) error { return os.ErrInvalid }
func (c *serverPacketConn) NeedAdditionalReadDeadline() bool { return true }
