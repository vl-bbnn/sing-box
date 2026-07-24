package v2rayxhttp

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/transport/v2rayhttp"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	aTLS "github.com/sagernet/sing/common/tls"
	sHTTP "github.com/sagernet/sing/protocol/http"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	serverMaxUploadBytes   = 1_000_000
	serverMaxBufferedPosts = 30
	serverSessionTTL       = 30 * time.Second
)

var (
	errUploadSequenceConsumed  = errors.New("xhttp upload sequence already consumed")
	errUploadSequenceDuplicate = errors.New("xhttp upload sequence already queued")
	errUploadQueueFull         = errors.New("xhttp upload queue is full")
)

var _ adapter.V2RayServerTransport = (*Server)(nil)

type serverSession struct {
	queue     *uploadQueue
	access    sync.Mutex
	active    bool
	closed    bool
	reaper    *time.Timer
	closeOnce sync.Once
}

func (s *serverSession) activate() bool {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed || s.active {
		return false
	}
	s.active = true
	if s.reaper != nil {
		s.reaper.Stop()
	}
	return true
}

func (s *serverSession) Close() {
	s.closeOnce.Do(func() {
		s.access.Lock()
		s.closed = true
		if s.reaper != nil {
			s.reaper.Stop()
		}
		s.access.Unlock()
		_ = s.queue.Close()
	})
}

type Server struct {
	ctx        context.Context
	logger     logger.ContextLogger
	tlsConfig  tls.ServerConfig
	handler    adapter.V2RayServerTransportHandler
	httpServer *http.Server
	h2Server   *http2.Server
	h2cHandler http.Handler
	host       string
	path       string
	paddingMin int
	paddingMax int
	sessionTTL time.Duration
	localAddr  net.Addr
	sessions   sync.Map
}

func NewServer(ctx context.Context, logger logger.ContextLogger, options option.V2RayXHTTPOptions, tlsConfig tls.ServerConfig, handler adapter.V2RayServerTransportHandler) (*Server, error) {
	mode := options.Mode
	if mode == "" {
		mode = modePacketUp
	}
	if mode != modePacketUp {
		return nil, E.New("v2ray-xhttp server: unsupported mode: ", mode)
	}
	paddingMin, paddingMax, err := parsePaddingRange(options.XPaddingBytes)
	if err != nil {
		return nil, err
	}
	path := "/" + strings.Trim(options.Path, "/")
	if path == "/" && options.Path == "" {
		path = "/"
	}
	server := &Server{
		ctx:        ctx,
		logger:     logger,
		tlsConfig:  tlsConfig,
		handler:    handler,
		h2Server:   &http2.Server{},
		host:       strings.ToLower(options.Host),
		path:       path,
		paddingMin: paddingMin,
		paddingMax: paddingMax,
		sessionTTL: serverSessionTTL,
	}
	server.httpServer = &http.Server{
		Handler:           server,
		ReadHeaderTimeout: C.TCPTimeout,
		MaxHeaderBytes:    http.DefaultMaxHeaderBytes,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
		ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
			return log.ContextWithNewID(ctx)
		},
	}
	server.h2cHandler = h2c.NewHandler(server, server.h2Server)
	return server, nil
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method == "PRI" && len(request.Header) == 0 && request.URL.Path == "*" && request.Proto == "HTTP/2.0" {
		s.h2cHandler.ServeHTTP(writer, request)
		return
	}
	if s.host != "" && strings.ToLower(stripHostPort(request.Host)) != s.host {
		s.invalidRequest(writer, request, http.StatusNotFound, E.New("bad host: ", request.Host))
		return
	}
	if !s.validPadding(request) {
		s.invalidRequest(writer, request, http.StatusBadRequest, errors.New("invalid x_padding"))
		return
	}
	sessionID, seq, hasSeq, validPath := s.parsePath(request.URL.Path)
	if !validPath {
		s.invalidRequest(writer, request, http.StatusNotFound, E.New("bad path: ", request.URL.Path))
		return
	}
	switch request.Method {
	case http.MethodGet:
		if hasSeq {
			s.invalidRequest(writer, request, http.StatusMethodNotAllowed, errors.New("download path contains sequence"))
			return
		}
		s.serveDownload(writer, request, sessionID)
	case http.MethodPost:
		if !hasSeq {
			s.invalidRequest(writer, request, http.StatusMethodNotAllowed, errors.New("upload path lacks sequence"))
			return
		}
		s.serveUpload(writer, request, sessionID, seq)
	default:
		s.invalidRequest(writer, request, http.StatusMethodNotAllowed, E.New("unsupported method: ", request.Method))
	}
}

func (s *Server) serveDownload(writer http.ResponseWriter, request *http.Request, sessionID string) {
	session := s.loadOrCreateSession(sessionID)
	if !session.activate() {
		s.invalidRequest(writer, request, http.StatusConflict, errors.New("download already active"))
		return
	}
	s.writeResponseHeaders(writer)
	writer.WriteHeader(http.StatusOK)
	flusher, loaded := writer.(http.Flusher)
	if !loaded {
		s.deleteSession(sessionID, session)
		return
	}
	flusher.Flush()

	done := make(chan struct{})
	var doneOnce sync.Once
	closeDone := func(error) {
		doneOnce.Do(func() { close(done) })
	}
	conn := &serverPacketConn{
		reader:     session.queue,
		writer:     writer,
		flusher:    flusher,
		localAddr:  s.localAddr,
		remoteAddr: M.SocksaddrFromNet(sHTTP.SourceAddress(request).TCPAddr()),
	}
	s.handler.NewConnectionEx(v2rayhttp.DupContext(request.Context()), conn, sHTTP.SourceAddress(request), M.Socksaddr{}, N.OnceClose(closeDone))
	select {
	case <-request.Context().Done():
	case <-done:
	}
	_ = conn.Close()
	s.deleteSession(sessionID, session)
}

func (s *Server) serveUpload(writer http.ResponseWriter, request *http.Request, sessionID string, seq uint64) {
	if request.ContentLength > serverMaxUploadBytes {
		s.invalidRequest(writer, request, http.StatusRequestEntityTooLarge, errors.New("upload is too large"))
		return
	}
	payload, err := io.ReadAll(io.LimitReader(request.Body, serverMaxUploadBytes+1))
	if err != nil {
		s.invalidRequest(writer, request, http.StatusBadRequest, E.Cause(err, "read upload"))
		return
	}
	if len(payload) > serverMaxUploadBytes {
		s.invalidRequest(writer, request, http.StatusRequestEntityTooLarge, errors.New("upload is too large"))
		return
	}
	session := s.loadOrCreateSession(sessionID)
	err = session.queue.Push(seq, payload)
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, errUploadQueueFull) {
			status = http.StatusTooManyRequests
		}
		s.invalidRequest(writer, request, status, err)
		return
	}
	s.writeResponseHeaders(writer)
	writer.WriteHeader(http.StatusOK)
}

func (s *Server) loadOrCreateSession(sessionID string) *serverSession {
	created := &serverSession{queue: newUploadQueue(serverMaxBufferedPosts)}
	created.access.Lock()
	current, loaded := s.sessions.LoadOrStore(sessionID, created)
	if loaded {
		created.access.Unlock()
		return current.(*serverSession)
	}
	created.reaper = time.AfterFunc(s.sessionTTL, func() {
		s.deleteSession(sessionID, created)
	})
	created.access.Unlock()
	return created
}

func (s *Server) deleteSession(sessionID string, session *serverSession) {
	current, loaded := s.sessions.Load(sessionID)
	if loaded && current == session {
		s.sessions.Delete(sessionID)
	}
	session.Close()
}

func (s *Server) parsePath(requestPath string) (string, uint64, bool, bool) {
	prefix := s.path
	if prefix == "/" {
		prefix = ""
	}
	if !strings.HasPrefix(requestPath, prefix+"/") {
		return "", 0, false, false
	}
	remainder := strings.TrimPrefix(requestPath, prefix+"/")
	parts := strings.Split(remainder, "/")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" {
		return "", 0, false, false
	}
	if len(parts) == 1 {
		return parts[0], 0, false, true
	}
	if parts[1] == "" {
		return "", 0, false, false
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return "", 0, false, false
	}
	return parts[0], seq, true, true
}

func (s *Server) validPadding(request *http.Request) bool {
	referer := request.Header.Get("Referer")
	parsed, err := url.Parse(referer)
	if err != nil || referer == "" {
		return false
	}
	padding := parsed.Query().Get("x_padding")
	return len(padding) >= s.paddingMin && len(padding) <= s.paddingMax
}

func (s *Server) writeResponseHeaders(writer http.ResponseWriter) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Accel-Buffering", "no")
	length := s.paddingMin
	if s.paddingMax > s.paddingMin {
		length += rand.Intn(s.paddingMax - s.paddingMin + 1)
	}
	writer.Header().Set("X-Padding", strings.Repeat("0", length))
}

func (s *Server) invalidRequest(writer http.ResponseWriter, request *http.Request, statusCode int, err error) {
	writer.WriteHeader(statusCode)
	s.logger.ErrorContext(request.Context(), E.Cause(err, "process XHTTP request from ", request.RemoteAddr))
}

func stripHostPort(host string) string {
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		return parsedHost
	}
	return host
}

func (s *Server) Network() []string {
	return []string{N.NetworkTCP}
}

func (s *Server) Serve(listener net.Listener) error {
	s.localAddr = listener.Addr()
	if s.tlsConfig != nil {
		if len(s.tlsConfig.NextProtos()) == 0 {
			s.tlsConfig.SetNextProtos([]string{http2.NextProtoTLS, "http/1.1"})
		} else if !common.Contains(s.tlsConfig.NextProtos(), http2.NextProtoTLS) {
			s.tlsConfig.SetNextProtos(append([]string{http2.NextProtoTLS}, s.tlsConfig.NextProtos()...))
		}
		listener = aTLS.NewListener(listener, s.tlsConfig)
	}
	return s.httpServer.Serve(listener)
}

func (s *Server) ServePacket(net.PacketConn) error {
	return os.ErrInvalid
}

func (s *Server) Close() error {
	s.sessions.Range(func(key, value any) bool {
		s.deleteSession(key.(string), value.(*serverSession))
		return true
	})
	return s.httpServer.Close()
}
