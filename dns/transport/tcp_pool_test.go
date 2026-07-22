package transport

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	boxdns "github.com/sagernet/sing-box/dns"
	M "github.com/sagernet/sing/common/metadata"

	mDNS "github.com/miekg/dns"
)

type tcpPoolTestDialer struct {
	address string
}

func (d tcpPoolTestDialer) DialContext(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

func (tcpPoolTestDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, net.ErrClosed
}

func TestTCPTransportUsesTwoMultiplexedConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var accepted atomic.Int32
	serverDone := make(chan struct{})
	var serverConnections sync.WaitGroup
	go func() {
		defer close(serverDone)
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			accepted.Add(1)
			serverConnections.Add(1)
			go func() {
				defer serverConnections.Done()
				defer conn.Close()
				for {
					request, readErr := ReadMessage(conn)
					if readErr != nil {
						return
					}
					response := new(mDNS.Msg)
					response.SetReply(request)
					if writeErr := WriteMessage(conn, request.Id, response); writeErr != nil {
						return
					}
				}
			}()
		}
	}()

	transport := NewTCPRaw(boxdns.NewTransportAdapter("tcp", "test", nil), tcpPoolTestDialer{address: listener.Addr().String()}, M.Socksaddr{})
	defer func() {
		transport.Close()
		listener.Close()
		<-serverDone
		serverConnections.Wait()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results := make(chan error, 4)
	for index := 0; index < cap(results); index++ {
		message := new(mDNS.Msg)
		message.SetQuestion("example.com.", mDNS.TypeA)
		transport.ExchangeAsync(ctx, message, func(_ *mDNS.Msg, callbackErr error) {
			results <- callbackErr
		})
	}
	for index := 0; index < cap(results); index++ {
		if callbackErr := <-results; callbackErr != nil {
			t.Fatal(callbackErr)
		}
	}
	if connections := accepted.Load(); connections != tcpMultiplexerCount {
		t.Fatalf("expected %d multiplexed connections, got %d", tcpMultiplexerCount, connections)
	}
}
