//go:build android && with_wlt

package libbox

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Only netlink bytes are inspected. LinkDeserialize's optional TUN sysfs
// fallback is not available to Android apps and causes target-UID AVC spam.
func startPhysicalLinkObserver(m *platformDefaultInterfaceMonitor) (func(), error) {
	startedAt := time.Now()
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, unix.NETLINK_ROUTE)
	if err != nil {
		return nil, err
	}
	if err = unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK, Groups: 1 << (unix.RTNLGRP_LINK - 1)}); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	// os.File's runtime poller makes Close wake a pending nonblocking receive.
	// A bare close(2) cannot reliably interrupt Recvfrom on another goroutine.
	socket := os.NewFile(uintptr(fd), "wlt-link-observer")
	raw, err := socket.SyscallConn()
	if err != nil {
		_ = socket.Close()
		return nil, err
	}
	buffer := make([]byte, 64*1024)
	receive := func() (physicalLinkDatagram, error) {
		var packet physicalLinkDatagram
		var receiveErr error
		var flags, count int
		var sender unix.Sockaddr
		pollErr := raw.Read(func(descriptor uintptr) bool {
			for {
				count, _, flags, sender, receiveErr = unix.Recvmsg(int(descriptor), buffer, nil, 0)
				if errors.Is(receiveErr, unix.EINTR) {
					continue
				}
				break
			}
			if errors.Is(receiveErr, unix.EAGAIN) {
				return false
			}
			if receiveErr == nil {
				packet.ObservedAt = time.Now()
				// Snapshot before parsing/log delivery so queue or logger latency cannot
				// compare an old event against a later platform default interface.
				packet.Current, packet.InterfaceRevision = m.physicalLinkSnapshot()
			}
			return true
		})
		if pollErr != nil {
			return packet, pollErr
		}
		if receiveErr != nil {
			return packet, receiveErr
		}
		if flags&unix.MSG_TRUNC != 0 {
			return packet, fmt.Errorf("truncated netlink datagram")
		}
		peer, ok := sender.(*unix.SockaddrNetlink)
		if !ok || peer.Pid != 0 {
			return packet, fmt.Errorf("non-kernel netlink sender")
		}
		packet.Data = buffer[:count]
		return packet, nil
	}
	var eventSequence uint64 // Owned by the single reader goroutine.
	emit := func(observation physicalLinkObservation) {
		eventSequence++
		// Abort the obsolete carrier before diagnostic logging can delay loss.
		forwarded := m.forwardPhysicalLinkLoss(observation)
		m.logger.Info(
			"android physical link observer event event_seq=", eventSequence,
			" monotonic_ns=", observation.MonotonicNanos,
			" wall_unix_ms=", observation.WallUnixMillis,
			" message_type=", observation.MessageType,
			" ifindex=", observation.InterfaceIndex,
			" interface=", observation.InterfaceName,
			" raw_flags=0x", strconv.FormatUint(uint64(observation.RawFlags), 16),
			" change=0x", strconv.FormatUint(uint64(observation.Change), 16),
			" admin_up=", observation.AdminUp,
			" lower_up=", observation.LowerUp,
			" deleted=", observation.Deleted,
			" current_ifindex=", observation.CurrentInterfaceIndex,
			" current_interface=", observation.CurrentInterfaceName,
			" current_match=", observation.CurrentMatch,
			" candidate_loss=", observation.CandidateLoss,
			" interface_revision=", observation.InterfaceRevision,
			" loss_forwarded=", forwarded,
			" diagnostic_only=false",
		)
	}
	// Publish the lifetime boundary before the reader can emit its first event.
	m.logger.Info("android physical link observer started loss_forwarding=true")
	reader := startPhysicalLinkReader(startedAt, receive, func() { _ = socket.Close() }, emit, func(err error) {
		m.logger.Warn("android physical link observer receive_error=", err, " loss_forwarding=true")
	})
	var closeOnce sync.Once
	return func() {
		closeOnce.Do(func() {
			reader.close()
			m.logger.Info("android physical link observer stopped loss_forwarding=true")
		})
	}, nil
}
