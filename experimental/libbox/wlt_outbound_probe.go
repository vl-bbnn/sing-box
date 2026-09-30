//go:build with_wlt

package libbox

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	stdJSON "encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	dnsTransport "github.com/sagernet/sing-box/dns/transport"
	M "github.com/sagernet/sing/common/metadata"

	mDNS "github.com/miekg/dns"
)

const (
	wltProbeScope          = "explicit_saved_WLT_outbound_reachability"
	wltProbeMaxInputBytes  = 16 * 1024
	wltProbeMaxOutputBytes = 16 * 1024
	wltProbeMaxDNSBytes    = 16 * 1024
	wltProbeMaxHTTPHeader  = 16 * 1024
	wltProbeMaxCNAMEHops   = 16
	wltProbeBulkBytes      = int64(1_048_576)
)

var (
	wltProbeIDPattern      = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	wltProbeTagPattern     = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	wltProbeBulkURLPattern = regexp.MustCompile(
		`^https://speed\.cloudflare\.com/__down\?bytes=1048576&seed=[A-Za-z0-9._-]{1,64}$`,
	)
	wltProbeBusy sync.Map
)

type wltProbeRequest struct {
	Schema         int    `json:"schema"`
	ProbeID        string `json:"probe_id"`
	Kind           string `json:"kind"`
	GroupTag       string `json:"group_tag"`
	OutboundTag    string `json:"outbound_tag"`
	WLTTag         string `json:"wlt_tag"`
	TimeoutMS      int    `json:"timeout_ms"`
	URL            string `json:"url"`
	ExpectedStatus int    `json:"expected_status"`
	ExpectedBytes  int64  `json:"expected_bytes"`
	MaxReadBytes   int64  `json:"max_read_bytes"`
	Server         string `json:"server"`
	QueryName      string `json:"query_name"`
}

type wltProbeResult struct {
	Schema            int    `json:"schema"`
	Scope             string `json:"scope"`
	ProbeID           string `json:"probe_id"`
	Kind              string `json:"kind"`
	Status            string `json:"status"`
	ErrorCode         string `json:"error_code"`
	GroupTag          string `json:"group_tag"`
	OutboundTag       string `json:"outbound_tag"`
	WLTTag            string `json:"wlt_tag"`
	Network           string `json:"network"`
	Attempt           string `json:"attempt"`
	FallbackAttempted bool   `json:"fallback_attempted"`
	SelectionTouched  bool   `json:"selection_touched"`
	ProfileTouched    bool   `json:"profile_touched"`
	InstanceCurrent   bool   `json:"instance_current"`
	DurationMS        int64  `json:"duration_ms"`
	RequestSHA256     string `json:"request_sha256"`
	HTTPStatus        *int   `json:"http_status,omitempty"`
	BytesRead         *int64 `json:"bytes_read,omitempty"`
	DestinationSHA256 string `json:"destination_sha256,omitempty"`
	DNSRCode          *int   `json:"dns_rcode,omitempty"`
	DNSAnswerCount    *int   `json:"dns_answer_count,omitempty"`
	DNSQuestionSHA256 string `json:"dns_question_sha256,omitempty"`
	DNSServerSHA256   string `json:"dns_server_sha256,omitempty"`
}

type wltOutboundResolver interface {
	Outbound(tag string) (adapter.Outbound, bool)
}

// ProbeWltOutbound runs one bounded diagnostic through a concrete VLESS leaf
// in the active instance. It never dials the containing group or updates group
// selection, URLTest history, profile content, or runtime configuration.
func (s *CommandServer) ProbeWltOutbound(requestJSON string) (string, error) {
	request, requestHash, err := parseWLTProbeRequest(requestJSON)
	if err != nil {
		return "", err
	}
	instance := s.StartedService.Instance()
	if instance == nil || instance.Box() == nil {
		return "", newWLTProbeError("active instance unavailable")
	}
	return executeWLTProbe(
		instance,
		instance.Context(),
		instance.Box().Outbound(),
		func() bool { return s.StartedService.Instance() == instance },
		request,
		requestHash,
	)
}

func parseWLTProbeRequest(requestJSON string) (wltProbeRequest, string, error) {
	requestBytes := []byte(requestJSON)
	requestHash := sha256Hex(requestBytes)
	if len(requestBytes) == 0 || len(requestBytes) > wltProbeMaxInputBytes {
		return wltProbeRequest{}, requestHash, newWLTProbeError("invalid request size")
	}
	fields, err := decodeWLTProbeFields(requestBytes)
	if err != nil {
		return wltProbeRequest{}, requestHash, newWLTProbeError("invalid request JSON")
	}
	var request wltProbeRequest
	decoder := stdJSON.NewDecoder(strings.NewReader(requestJSON))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return wltProbeRequest{}, requestHash, newWLTProbeError("invalid request fields")
	}
	if request.Schema != 1 || !wltProbeIDPattern.MatchString(request.ProbeID) ||
		!wltProbeTagPattern.MatchString(request.GroupTag) ||
		!wltProbeTagPattern.MatchString(request.OutboundTag) ||
		!wltProbeTagPattern.MatchString(request.WLTTag) ||
		request.TimeoutMS < 1 || request.TimeoutMS > 90_000 {
		return wltProbeRequest{}, requestHash, newWLTProbeError("invalid common request contract")
	}
	commonFields := []string{
		"schema", "probe_id", "kind", "group_tag", "outbound_tag", "wlt_tag", "timeout_ms",
	}
	switch request.Kind {
	case "https":
		if !exactJSONFields(fields, append(commonFields,
			"url", "expected_status", "expected_bytes", "max_read_bytes")) ||
			!validWLTHTTPSContract(request) {
			return wltProbeRequest{}, requestHash, newWLTProbeError("invalid HTTPS request contract")
		}
	case "dns":
		if !exactJSONFields(fields, append(commonFields, "server", "query_name")) ||
			!validWLTDNSContract(request) {
			return wltProbeRequest{}, requestHash, newWLTProbeError("invalid DNS request contract")
		}
	default:
		return wltProbeRequest{}, requestHash, newWLTProbeError("invalid probe kind")
	}
	return request, requestHash, nil
}

func decodeWLTProbeFields(requestBytes []byte) (map[string]stdJSON.RawMessage, error) {
	decoder := stdJSON.NewDecoder(strings.NewReader(string(requestBytes)))
	start, err := decoder.Token()
	if err != nil || start != stdJSON.Delim('{') {
		return nil, errors.New("request must be a JSON object")
	}
	fields := make(map[string]stdJSON.RawMessage)
	for decoder.More() {
		nameToken, tokenErr := decoder.Token()
		if tokenErr != nil {
			return nil, tokenErr
		}
		name, isString := nameToken.(string)
		if !isString {
			return nil, errors.New("request field name must be a string")
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, errors.New("duplicate request field")
		}
		var value stdJSON.RawMessage
		if decodeErr := decoder.Decode(&value); decodeErr != nil {
			return nil, decodeErr
		}
		fields[name] = value
	}
	end, err := decoder.Token()
	if err != nil || end != stdJSON.Delim('}') {
		return nil, errors.New("invalid request object")
	}
	if _, err = decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing request content")
	}
	return fields, nil
}

func exactJSONFields(fields map[string]stdJSON.RawMessage, expected []string) bool {
	if len(fields) != len(expected) {
		return false
	}
	for _, name := range expected {
		if _, found := fields[name]; !found {
			return false
		}
	}
	return true
}

func validWLTHTTPSContract(request wltProbeRequest) bool {
	parsed, err := url.Parse(request.URL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Fragment != "" ||
		parsed.Hostname() == "" {
		return false
	}
	switch {
	case request.URL == "https://cp.cloudflare.com/generate_204":
		return request.ExpectedStatus == http.StatusNoContent && request.ExpectedBytes == 0 &&
			request.MaxReadBytes == 1
	case wltProbeBulkURLPattern.MatchString(request.URL):
		return request.ExpectedStatus == http.StatusOK && request.ExpectedBytes == wltProbeBulkBytes &&
			request.MaxReadBytes == wltProbeBulkBytes+1
	default:
		return false
	}
}

func validWLTDNSContract(request wltProbeRequest) bool {
	server, err := netip.ParseAddrPort(request.Server)
	return err == nil && server.Port() != 0 && server.Addr().Zone() == "" && normalizeWLTQueryName(request.QueryName) != ""
}

func normalizeWLTQueryName(name string) string {
	name = strings.ToLower(strings.TrimSuffix(name, "."))
	if len(name) == 0 || len(name) > 253 {
		return ""
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return ""
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return ""
			}
		}
	}
	return name
}

func executeWLTProbe(
	instanceKey any,
	instanceCtx context.Context,
	resolver wltOutboundResolver,
	instanceCurrent func() bool,
	request wltProbeRequest,
	requestHash string,
) (string, error) {
	if _, busy := wltProbeBusy.LoadOrStore(instanceKey, struct{}{}); busy {
		return "", newWLTProbeError("probe already active for instance")
	}
	workerOwnsBusy := false
	defer func() {
		if !workerOwnsBusy {
			wltProbeBusy.Delete(instanceKey)
		}
	}()
	groupOutbound, loaded := resolver.Outbound(request.GroupTag)
	if !loaded {
		return "", newWLTProbeError("probe group unavailable")
	}
	group, isGroup := groupOutbound.(adapter.OutboundGroup)
	if !isGroup || !slices.Contains(group.All(), request.OutboundTag) {
		return "", newWLTProbeError("invalid probe group membership")
	}
	leaf, loaded := resolver.Outbound(request.OutboundTag)
	if !loaded || leaf.Type() != C.TypeVLESS || len(leaf.Dependencies()) != 1 ||
		leaf.Dependencies()[0] != request.WLTTag || !slices.Contains(leaf.Network(), "tcp") {
		return "", newWLTProbeError("invalid probe VLESS leaf")
	}
	if _, nested := leaf.(adapter.OutboundGroup); nested {
		return "", newWLTProbeError("probe leaf must be concrete")
	}
	wltOutbound, loaded := resolver.Outbound(request.WLTTag)
	if !loaded || wltOutbound.Type() != C.TypeWLT {
		return "", newWLTProbeError("invalid probe WLT dependency")
	}
	if _, nested := wltOutbound.(adapter.OutboundGroup); nested {
		return "", newWLTProbeError("WLT dependency must be concrete")
	}

	ctx, cancel := context.WithTimeout(instanceCtx, time.Duration(request.TimeoutMS)*time.Millisecond)
	defer cancel()
	started := time.Now()
	completed := make(chan wltProbeResult, 1)
	workerOwnsBusy = true
	go func() {
		result := newWLTProbeResult(request, requestHash)
		if request.Kind == "https" {
			runWLTHTTPSProbe(ctx, leaf, request, &result)
		} else {
			runWLTDNSProbe(ctx, leaf, request, &result)
		}
		finishWLTProbeResult(&result, ctx, instanceCurrent, started, "timeout")
		wltProbeBusy.Delete(instanceKey)
		completed <- result
	}()

	select {
	case result := <-completed:
		finishWLTProbeResult(&result, ctx, instanceCurrent, started, "timeout")
		return encodeWLTProbeResult(result)
	case <-ctx.Done():
		select {
		case result := <-completed:
			finishWLTProbeResult(&result, ctx, instanceCurrent, started, "timeout")
			return encodeWLTProbeResult(result)
		default:
			result := newWLTProbeResult(request, requestHash)
			finishWLTProbeResult(&result, ctx, instanceCurrent, started, "timeout_cleanup_pending")
			return encodeWLTProbeResult(result)
		}
	}
}

func finishWLTProbeResult(result *wltProbeResult, ctx context.Context, instanceCurrent func() bool, started time.Time, timeoutCode string) {
	result.InstanceCurrent = instanceCurrent()
	timedOut := ctx.Err() != nil
	if deadline, exists := ctx.Deadline(); exists && !time.Now().Before(deadline) {
		timedOut = true
	}
	if !result.InstanceCurrent {
		result.Status = "failed"
		result.ErrorCode = "instance_changed"
	} else if timedOut {
		result.Status = "failed"
		result.ErrorCode = timeoutCode
	}
	result.DurationMS = max(time.Since(started).Milliseconds(), 0)
}

func newWLTProbeResult(request wltProbeRequest, requestHash string) wltProbeResult {
	result := wltProbeResult{
		Schema: 1, Scope: wltProbeScope, ProbeID: request.ProbeID, Kind: request.Kind,
		Status: "failed", GroupTag: request.GroupTag, OutboundTag: request.OutboundTag,
		WLTTag: request.WLTTag, Network: "tcp", Attempt: "primary",
		RequestSHA256: requestHash, InstanceCurrent: true,
	}
	if request.Kind == "https" {
		httpStatus := 0
		bytesRead := int64(0)
		result.HTTPStatus = &httpStatus
		result.BytesRead = &bytesRead
		result.DestinationSHA256 = sha256Hex([]byte(request.URL))
	} else {
		rcode := -1
		answerCount := 0
		result.DNSRCode = &rcode
		result.DNSAnswerCount = &answerCount
		result.DNSQuestionSHA256 = sha256Hex([]byte(normalizeWLTQueryName(request.QueryName)))
		result.DNSServerSHA256 = sha256Hex([]byte(request.Server))
	}
	return result
}

func runWLTHTTPSProbe(ctx context.Context, leaf adapter.Outbound, request wltProbeRequest, result *wltProbeResult) {
	parsed, _ := url.Parse(request.URL)
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	conn, err := leaf.DialContext(ctx, "tcp", M.ParseSocksaddrHostPortStr(parsed.Hostname(), port))
	if err != nil {
		result.ErrorCode = contextWLTErrorCode(ctx, "https_request_failed")
		return
	}
	defer closeWLTProbeConn(conn)
	defer closeWLTProbeConnOnContext(ctx, conn)()
	if deadline, exists := ctx.Deadline(); exists {
		if err = conn.SetDeadline(deadline); err != nil {
			result.ErrorCode = "deadline_unavailable"
			return
		}
	}
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName: parsed.Hostname(), RootCAs: adapter.RootPoolFromContext(ctx), MinVersion: tls.VersionTLS12,
	})
	if err = tlsConn.HandshakeContext(ctx); err != nil {
		result.ErrorCode = contextWLTErrorCode(ctx, "https_request_failed")
		return
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, request.URL, nil)
	if err != nil {
		result.ErrorCode = "https_request_invalid"
		return
	}
	httpRequest.Close = true
	if err = httpRequest.Write(tlsConn); err != nil {
		result.ErrorCode = contextWLTErrorCode(ctx, "https_request_failed")
		return
	}
	responseReader := bufio.NewReader(&wltProbeHTTPHeaderReader{
		reader: tlsConn, remaining: wltProbeMaxHTTPHeader, inHeader: true,
	})
	response, err := http.ReadResponse(responseReader, httpRequest)
	if err != nil {
		result.ErrorCode = contextWLTErrorCode(ctx, "https_request_failed")
		return
	}
	// This is an owned single-use socket. Close it before Body.Close so an
	// unread or excess response can never be drained beyond MaxReadBytes.
	defer response.Body.Close()
	defer closeWLTProbeConn(conn)
	*result.HTTPStatus = response.StatusCode
	var read int64
	if request.ExpectedBytes == 0 {
		_, err = responseReader.ReadByte()
		if err == nil {
			read = 1
		} else if errors.Is(err, io.EOF) {
			err = nil
		}
	} else {
		read, err = io.Copy(io.Discard, io.LimitReader(response.Body, request.MaxReadBytes))
	}
	*result.BytesRead = read
	if err != nil {
		result.ErrorCode = contextWLTErrorCode(ctx, "https_read_failed")
		return
	}
	if response.StatusCode != request.ExpectedStatus {
		result.ErrorCode = "https_status_mismatch"
		return
	}
	if read != request.ExpectedBytes {
		result.ErrorCode = "https_bytes_mismatch"
		return
	}
	result.Status = "success"
}

type wltProbeHTTPHeaderReader struct {
	reader    io.Reader
	remaining int
	tail      [4]byte
	tailSize  int
	inHeader  bool
}

func (r *wltProbeHTTPHeaderReader) Read(buffer []byte) (int, error) {
	if r.inHeader {
		if r.remaining == 0 {
			return 0, errors.New("HTTP response header exceeds size bound")
		}
		buffer = buffer[:min(len(buffer), r.remaining)]
	}
	read, err := r.reader.Read(buffer)
	if r.inHeader {
		r.remaining -= read
		for _, character := range buffer[:read] {
			if r.tailSize < len(r.tail) {
				r.tail[r.tailSize] = character
				r.tailSize++
			} else {
				copy(r.tail[:], r.tail[1:])
				r.tail[len(r.tail)-1] = character
			}
			if r.tailSize == len(r.tail) && r.tail == [4]byte{'\r', '\n', '\r', '\n'} {
				r.inHeader = false
				break
			}
		}
	}
	return read, err
}

func runWLTDNSProbe(ctx context.Context, leaf adapter.Outbound, request wltProbeRequest, result *wltProbeResult) {
	server, _ := netip.ParseAddrPort(request.Server)
	conn, err := leaf.DialContext(ctx, "tcp", M.SocksaddrFrom(server.Addr(), server.Port()))
	if err != nil {
		result.ErrorCode = contextWLTErrorCode(ctx, "dns_dial_failed")
		return
	}
	defer closeWLTProbeConn(conn)
	defer closeWLTProbeConnOnContext(ctx, conn)()
	if deadline, exists := ctx.Deadline(); exists {
		if err = conn.SetDeadline(deadline); err != nil {
			result.ErrorCode = "deadline_unavailable"
			return
		}
	}

	queryName := normalizeWLTQueryName(request.QueryName)
	query := new(mDNS.Msg)
	query.SetQuestion(mDNS.Fqdn(queryName), mDNS.TypeA)
	if err = dnsTransport.WriteMessage(conn, query.Id, query); err != nil {
		result.ErrorCode = contextWLTErrorCode(ctx, "dns_write_failed")
		return
	}
	response, err := readWLTProbeDNSMessage(conn)
	if err != nil {
		result.ErrorCode = contextWLTErrorCode(ctx, "dns_read_failed")
		return
	}
	*result.DNSRCode = response.Rcode
	if response.Id != query.Id {
		result.ErrorCode = "dns_id_mismatch"
		return
	}
	if !response.Response || response.Opcode != mDNS.OpcodeQuery || response.Truncated {
		result.ErrorCode = "dns_response_invalid"
		return
	}
	if len(response.Question) != 1 || !sameWLTQuestion(response.Question[0], query.Question[0]) {
		result.ErrorCode = "dns_question_mismatch"
		return
	}
	if response.Rcode != mDNS.RcodeSuccess {
		result.ErrorCode = "dns_rcode_mismatch"
		return
	}
	answerCount := matchingWLTAAnswers(response.Answer, queryName)
	*result.DNSAnswerCount = answerCount
	if answerCount == 0 {
		result.ErrorCode = "dns_answer_missing"
		return
	}
	result.Status = "success"
}

func sameWLTQuestion(left mDNS.Question, right mDNS.Question) bool {
	return strings.EqualFold(left.Name, right.Name) && left.Qtype == right.Qtype && left.Qclass == right.Qclass
}

func matchingWLTAAnswers(answers []mDNS.RR, queryName string) int {
	reachable := map[string]bool{mDNS.Fqdn(strings.ToLower(queryName)): true}
	for range wltProbeMaxCNAMEHops {
		var discovered []string
		for _, answer := range answers {
			cname, isCNAME := answer.(*mDNS.CNAME)
			if !isCNAME || cname.Hdr.Class != mDNS.ClassINET || !reachable[strings.ToLower(cname.Hdr.Name)] {
				continue
			}
			target := strings.ToLower(cname.Target)
			if !reachable[target] {
				discovered = append(discovered, target)
			}
		}
		if len(discovered) == 0 {
			break
		}
		for _, target := range discovered {
			reachable[target] = true
		}
	}
	count := 0
	for _, answer := range answers {
		if record, isA := answer.(*mDNS.A); isA && record.Hdr.Class == mDNS.ClassINET &&
			reachable[strings.ToLower(record.Hdr.Name)] && record.A.To4() != nil {
			count++
		}
	}
	return count
}

func closeWLTProbeConnOnContext(ctx context.Context, conn net.Conn) func() {
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = closeWLTProbeConn(conn)
		close(closed)
	})
	return func() {
		if !stop() {
			<-closed
		}
	}
}

func closeWLTProbeConn(conn net.Conn) error {
	current := any(conn)
	for range 16 {
		if aborter, supportsAbort := current.(interface{ Abort() error }); supportsAbort {
			return aborter.Abort()
		}
		upstream, canUnwrap := current.(interface{ Upstream() any })
		if !canUnwrap {
			break
		}
		next := upstream.Upstream()
		if next == nil {
			break
		}
		current = next
	}
	return conn.Close()
}

func readWLTProbeDNSMessage(reader io.Reader) (*mDNS.Msg, error) {
	var responseLength uint16
	if err := binary.Read(reader, binary.BigEndian, &responseLength); err != nil {
		return nil, err
	}
	if responseLength < 10 || responseLength > wltProbeMaxDNSBytes {
		return nil, mDNS.ErrShortRead
	}
	rawMessage := make([]byte, responseLength)
	if _, err := io.ReadFull(reader, rawMessage); err != nil {
		return nil, err
	}
	response := new(mDNS.Msg)
	if err := response.Unpack(rawMessage); err != nil {
		return nil, err
	}
	return response, nil
}

func contextWLTErrorCode(ctx context.Context, fallback string) string {
	if ctx.Err() != nil {
		return "timeout"
	}
	return fallback
}

func encodeWLTProbeResult(result wltProbeResult) (string, error) {
	encoded, err := stdJSON.Marshal(result)
	if err != nil {
		return "", newWLTProbeError("could not encode probe result")
	}
	if len(encoded) > wltProbeMaxOutputBytes {
		return "", newWLTProbeError("probe result exceeds size bound")
	}
	return string(encoded), nil
}

func sha256Hex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

type wltProbeError string

func (e wltProbeError) Error() string { return string(e) }

func newWLTProbeError(message string) error { return wltProbeError(message) }
