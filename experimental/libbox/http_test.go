package libbox

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPClientRetriesGETAfterTLSHandshakeTimeout(t *testing.T) {
	client := NewHTTPClient().(*httpClient)
	t.Cleanup(client.Close)
	require.True(t, client.transport.ForceAttemptHTTP2)

	transport := newTimeoutThenSuccessTransport()
	client.client.Transport = transport
	request := client.NewRequest()
	require.NoError(t, request.SetURL("https://example.com/config"))

	response, err := request.Execute()
	require.NoError(t, err)
	require.Equal(t, int32(2), transport.attempts.Load())
	content, err := response.GetContent()
	require.NoError(t, err)
	require.Equal(t, "ok", content.Value)
}

func TestHTTPClientDoesNotRetryPOSTAfterTLSHandshakeTimeout(t *testing.T) {
	client := NewHTTPClient().(*httpClient)
	t.Cleanup(client.Close)

	transport := newTimeoutThenSuccessTransport()
	client.client.Transport = transport
	request := client.NewRequest()
	require.NoError(t, request.SetURL("https://example.com/config"))
	request.SetMethod(http.MethodPost)

	_, err := request.Execute()
	require.Error(t, err)
	require.Equal(t, int32(1), transport.attempts.Load())
}

type timeoutThenSuccessTransport struct {
	attempts atomic.Int32
}

func newTimeoutThenSuccessTransport() *timeoutThenSuccessTransport {
	return new(timeoutThenSuccessTransport)
}

func (t *timeoutThenSuccessTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if t.attempts.Add(1) == 1 {
		return nil, tlsHandshakeTimeoutError{}
	}
	return &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("ok")),
		Request:    request,
	}, nil
}

type tlsHandshakeTimeoutError struct{}

func (tlsHandshakeTimeoutError) Error() string   { return "net/http: TLS handshake timeout" }
func (tlsHandshakeTimeoutError) Timeout() bool   { return true }
func (tlsHandshakeTimeoutError) Temporary() bool { return true }
