package route

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
)

func listenerNotifiedBeforeConnectionCloseCompletes(t *testing.T) bool {
	t.Helper()
	closeStarted := make(chan struct{})
	closeRelease := make(chan struct{})
	notified := make(chan struct{})
	resetDone := make(chan struct{})
	var releaseOnce sync.Once
	releaseClose := func() {
		releaseOnce.Do(func() {
			close(closeRelease)
		})
	}
	t.Cleanup(releaseClose)

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	connectionManager := NewConnectionManager(nil)
	connectionManager.TrackConn(&blockingCloseConn{
		Conn:    clientConn,
		started: closeStarted,
		release: closeRelease,
	})
	networkManager := &NetworkManager{
		connectionManager: connectionManager,
		endpoint:          &staticEndpointManager{},
		inbound:           &staticInboundManager{},
		outbound: &staticOutboundManager{outbounds: []adapter.Outbound{
			&interfaceUpdateOutbound{notified: notified},
		}},
	}

	go func() {
		networkManager.ResetNetwork()
		close(resetDone)
	}()

	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tracked connection close")
	}
	notifiedBeforeCloseCompleted := false
	select {
	case <-notified:
		notifiedBeforeCloseCompleted = true
	default:
	}
	releaseClose()
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for network reset")
	}
	select {
	case <-notified:
	default:
		t.Fatal("interface listener was not notified during network reset")
	}
	return notifiedBeforeCloseCompleted
}

func priorityListenerNotifiedBeforeEarlierListenerCompletes(t *testing.T) bool {
	t.Helper()
	listenerStarted := make(chan struct{})
	listenerRelease := make(chan struct{})
	priorityNotified := make(chan struct{})
	resetDone := make(chan struct{})
	var releaseOnce sync.Once
	releaseListener := func() {
		releaseOnce.Do(func() {
			close(listenerRelease)
		})
	}
	t.Cleanup(releaseListener)

	networkManager := &NetworkManager{
		endpoint: &staticEndpointManager{},
		inbound:  &staticInboundManager{},
		outbound: &staticOutboundManager{outbounds: []adapter.Outbound{
			&blockingInterfaceUpdateOutbound{
				started: listenerStarted,
				release: listenerRelease,
			},
			&priorityInterfaceUpdateOutbound{
				interfaceUpdateOutbound: interfaceUpdateOutbound{notified: priorityNotified},
			},
		}},
	}

	go func() {
		networkManager.ResetNetwork()
		close(resetDone)
	}()

	select {
	case <-listenerStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for earlier interface listener")
	}
	priorityNotifiedBeforeEarlierListenerCompleted := false
	select {
	case <-priorityNotified:
		priorityNotifiedBeforeEarlierListenerCompleted = true
	default:
	}
	releaseListener()
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for priority interface reset")
	}
	select {
	case <-priorityNotified:
	default:
		t.Fatal("priority interface listener was not notified during network reset")
	}
	return priorityNotifiedBeforeEarlierListenerCompleted
}

func interfaceUpdateSequence() []string {
	var sequence []string
	record := func(name string) {
		sequence = append(sequence, name)
	}
	networkManager := &NetworkManager{
		endpoint: &staticEndpointManager{endpoints: []adapter.Endpoint{
			&sequenceEndpoint{name: "endpoint", record: record},
		}},
		inbound: &staticInboundManager{inbounds: []adapter.Inbound{
			&sequenceInbound{name: "inbound", record: record},
		}},
		outbound: &staticOutboundManager{outbounds: []adapter.Outbound{
			&sequenceOutbound{name: "ordinary", record: record},
			&prioritySequenceOutbound{sequenceOutbound: sequenceOutbound{name: "priority", record: record}},
		}},
	}
	networkManager.ResetNetwork()
	return sequence
}

type blockingCloseConn struct {
	net.Conn
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingCloseConn) Close() error {
	c.once.Do(func() {
		close(c.started)
		<-c.release
	})
	return c.Conn.Close()
}

type interfaceUpdateOutbound struct {
	adapter.Outbound
	notified chan struct{}
}

func (o *interfaceUpdateOutbound) InterfaceUpdated() {
	close(o.notified)
}

type blockingInterfaceUpdateOutbound struct {
	adapter.Outbound
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *blockingInterfaceUpdateOutbound) InterfaceUpdated() {
	o.once.Do(func() {
		close(o.started)
		<-o.release
	})
}

type priorityInterfaceUpdateOutbound struct {
	interfaceUpdateOutbound
}

func (*priorityInterfaceUpdateOutbound) PriorityInterfaceUpdate() {}

type networkUnavailableOutbound struct {
	adapter.Outbound
	notified int
}

func (o *networkUnavailableOutbound) InterfaceUpdated()   {}
func (o *networkUnavailableOutbound) NetworkUnavailable() { o.notified++ }

type priorityNetworkUnavailableOutbound struct {
	networkUnavailableOutbound
}

func (*priorityNetworkUnavailableOutbound) PriorityInterfaceUpdate() {}

type sequenceEndpoint struct {
	adapter.Endpoint
	name   string
	record func(string)
}

func (l *sequenceEndpoint) InterfaceUpdated() {
	l.record(l.name)
}

type sequenceInbound struct {
	adapter.Inbound
	name   string
	record func(string)
}

func (l *sequenceInbound) InterfaceUpdated() {
	l.record(l.name)
}

type sequenceOutbound struct {
	adapter.Outbound
	name   string
	record func(string)
}

func (l *sequenceOutbound) InterfaceUpdated() {
	l.record(l.name)
}

type prioritySequenceOutbound struct {
	sequenceOutbound
}

func (*prioritySequenceOutbound) PriorityInterfaceUpdate() {}

type staticEndpointManager struct {
	adapter.EndpointManager
	endpoints []adapter.Endpoint
}

func (m *staticEndpointManager) Endpoints() []adapter.Endpoint {
	return m.endpoints
}

type staticInboundManager struct {
	adapter.InboundManager
	inbounds []adapter.Inbound
}

func (m *staticInboundManager) Inbounds() []adapter.Inbound {
	return m.inbounds
}

type staticOutboundManager struct {
	adapter.OutboundManager
	outbounds []adapter.Outbound
}

func (m *staticOutboundManager) Outbounds() []adapter.Outbound {
	return m.outbounds
}
