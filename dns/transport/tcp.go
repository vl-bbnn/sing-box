package transport

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dialer"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	mDNS "github.com/miekg/dns"
)

var _ adapter.DNSTransport = (*TCPTransport)(nil)

func RegisterTCP(registry *dns.TransportRegistry) {
	dns.RegisterTransport[option.RemoteDNSServerOptions](registry, C.DNSTypeTCP, NewTCP)
}

type TCPTransport struct {
	dns.TransportAdapter
	dialer       N.Dialer
	serverAddr   M.Socksaddr
	multiplexers []*queryMultiplexer
	next         atomic.Uint32
}

const tcpMultiplexerCount = 2

func NewTCP(ctx context.Context, logger log.ContextLogger, tag string, options option.RemoteDNSServerOptions) (adapter.DNSTransport, error) {
	transportDialer, err := dns.NewRemoteDialer(ctx, options)
	if err != nil {
		return nil, err
	}
	serverAddr := options.DNSServerAddressOptions.Build()
	if serverAddr.Port == 0 {
		serverAddr.Port = 53
	}
	if !serverAddr.IsValid() {
		return nil, E.New("invalid server address: ", serverAddr)
	}
	return NewTCPRaw(dns.NewTransportAdapterWithRemoteOptions(C.DNSTypeTCP, tag, options), transportDialer, serverAddr), nil
}

func NewTCPRaw(adapter dns.TransportAdapter, dialer N.Dialer, serverAddr M.Socksaddr) *TCPTransport {
	t := &TCPTransport{
		TransportAdapter: adapter,
		dialer:           dialer,
		serverAddr:       serverAddr,
	}
	multiplexerOptions := queryMultiplexerOptions{
		dial: func(ctx context.Context) (net.Conn, error) {
			conn, err := t.dialer.DialContext(ctx, N.NetworkTCP, t.serverAddr)
			if err != nil {
				return nil, E.Cause(err, "dial TCP connection")
			}
			return conn, nil
		},
		write: func(conn net.Conn, message *mDNS.Msg, queryId uint16) error {
			return WriteMessage(conn, queryId, message)
		},
		readNext: func(conn net.Conn) (*mDNS.Msg, error) {
			return ReadMessage(conn)
		},
	}
	t.multiplexers = make([]*queryMultiplexer, tcpMultiplexerCount)
	for index := range t.multiplexers {
		t.multiplexers[index] = newQueryMultiplexer(multiplexerOptions)
	}
	return t
}

func (t *TCPTransport) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	return dialer.InitializeDetour(t.dialer)
}

func (t *TCPTransport) Close() error {
	errors := make([]error, 0, len(t.multiplexers))
	for _, multiplexer := range t.multiplexers {
		errors = append(errors, multiplexer.Close())
	}
	return E.Errors(errors...)
}

func (t *TCPTransport) Reset() {
	for _, multiplexer := range t.multiplexers {
		multiplexer.Reset()
	}
}

func (t *TCPTransport) Exchange(ctx context.Context, message *mDNS.Msg) (*mDNS.Msg, error) {
	return t.nextMultiplexer().Exchange(ctx, message)
}

func (t *TCPTransport) ExchangeAsync(ctx context.Context, message *mDNS.Msg, callback func(response *mDNS.Msg, err error)) {
	t.nextMultiplexer().ExchangeAsync(ctx, message, callback)
}

func (t *TCPTransport) nextMultiplexer() *queryMultiplexer {
	index := (t.next.Add(1) - 1) % uint32(len(t.multiplexers))
	return t.multiplexers[index]
}

func setConnDeadline(ctx context.Context, conn net.Conn, needClose bool) func() {
	if needClose {
		stop := context.AfterFunc(ctx, func() {
			conn.Close()
		})
		return func() { stop() }
	}
	if d, ok := ctx.Deadline(); ok {
		conn.SetDeadline(d)
		return func() { conn.SetDeadline(time.Time{}) }
	}
	return func() {}
}

func ReadMessage(reader io.Reader) (*mDNS.Msg, error) {
	var responseLen uint16
	err := binary.Read(reader, binary.BigEndian, &responseLen)
	if err != nil {
		return nil, err
	}
	if responseLen < 10 {
		return nil, mDNS.ErrShortRead
	}
	buffer := buf.NewSize(int(responseLen))
	defer buffer.Release()
	_, err = buffer.ReadFullFrom(reader, int(responseLen))
	if err != nil {
		return nil, err
	}
	var message mDNS.Msg
	err = message.Unpack(buffer.Bytes())
	return &message, err
}

func WriteMessage(writer io.Writer, messageId uint16, message *mDNS.Msg) error {
	requestLen := message.Len()
	buffer := buf.NewSize(3 + requestLen)
	defer buffer.Release()
	common.Must(binary.Write(buffer, binary.BigEndian, uint16(requestLen)))
	exMessage := *message
	exMessage.Id = messageId
	exMessage.Compress = true
	rawMessage, err := exMessage.PackBuffer(buffer.FreeBytes())
	if err != nil {
		return err
	}
	buffer.Truncate(2 + len(rawMessage))
	return common.Error(writer.Write(buffer.Bytes()))
}
