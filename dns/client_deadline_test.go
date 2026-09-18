package dns

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/sagernet/sing-box/adapter"
)

type deadlineTransport struct {
	calls    atomic.Int32
	exchange func(context.Context, *dns.Msg, int32) (*dns.Msg, error)
}

func (*deadlineTransport) Start(adapter.StartStage) error { return nil }
func (*deadlineTransport) Close() error                   { return nil }
func (*deadlineTransport) Type() string                   { return "test" }
func (*deadlineTransport) Tag() string                    { return "test" }
func (*deadlineTransport) Dependencies() []string         { return nil }
func (*deadlineTransport) Reset()                         {}
func (d *deadlineTransport) Exchange(ctx context.Context, m *dns.Msg) (*dns.Msg, error) {
	return d.exchange(ctx, m, d.calls.Add(1))
}
func deadlineQuestion() *dns.Msg {
	m := new(dns.Msg)
	m.SetQuestion("deadline.example.com.", dns.TypeA)
	return m
}

func TestDNSCacheWaitConsumesOriginalTimeout(t *testing.T) {
	for _, independent := range []bool{false, true} {
		t.Run(fmt.Sprint(independent), func(t *testing.T) {
			budget := 200 * time.Millisecond
			client := NewClient(ClientOptions{Timeout: budget, IndependentCache: independent})
			entered := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			var allowedDeadline time.Time
			transport := &deadlineTransport{}
			transport.exchange = func(ctx context.Context, _ *dns.Msg, n int32) (*dns.Msg, error) {
				if n == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
					}
					return nil, errors.New("leader unavailable")
				}
				actual, ok := ctx.Deadline()
				if !ok || actual.After(allowedDeadline) {
					t.Errorf("cache wait reset DNS budget: deadline=%s allowed=%s", actual, allowedDeadline)
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			go func() {
				_, err := client.Exchange(context.Background(), transport, deadlineQuestion(), adapter.DNSQueryOptions{}, nil)
				done <- err
			}()
			<-entered
			started := time.Now()
			allowedDeadline = started.Add(budget + 20*time.Millisecond)
			go func() { time.Sleep(70 * time.Millisecond); close(release) }()
			_, err := client.Exchange(context.Background(), transport, deadlineQuestion(), adapter.DNSQueryOptions{}, nil)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("follower error=%v", err)
			}
			if <-done == nil {
				t.Fatal("expected leader failure")
			}
			if transport.calls.Load() != 2 {
				t.Fatal("retry transport not exercised")
			}
		})
	}
}

func TestDNSCallerDeadlineAndCancellationPreserved(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelEarly), func(t *testing.T) {
			client := NewClient(ClientOptions{Timeout: time.Second})
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			expected, _ := ctx.Deadline()
			transport := &deadlineTransport{exchange: func(c context.Context, _ *dns.Msg, _ int32) (*dns.Msg, error) {
				d, ok := c.Deadline()
				if !ok || !d.Equal(expected) {
					t.Error("caller deadline changed")
				}
				if cancelEarly {
					cancel()
				}
				<-c.Done()
				return nil, c.Err()
			}}
			_, err := client.Exchange(ctx, transport, deadlineQuestion(), adapter.DNSQueryOptions{}, nil)
			target := context.DeadlineExceeded
			if cancelEarly {
				target = context.Canceled
			}
			if !errors.Is(err, target) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestDNSCacheHitDoesNotExchangeAndClonesMessageID(t *testing.T) {
	for _, independent := range []bool{false, true} {
		t.Run(fmt.Sprint(independent), func(t *testing.T) {
			client := NewClient(ClientOptions{Timeout: time.Second, IndependentCache: independent})
			transport := &deadlineTransport{exchange: func(ctx context.Context, m *dns.Msg, _ int32) (*dns.Msg, error) {
				response := new(dns.Msg)
				response.SetReply(m)
				record, err := dns.NewRR("deadline.example.com. 60 IN A 192.0.2.1")
				if err != nil {
					t.Fatal(err)
				}
				response.Answer = []dns.RR{record}
				return response, nil
			}}
			first := deadlineQuestion()
			first.Id = 100
			response, err := client.Exchange(context.Background(), transport, first, adapter.DNSQueryOptions{}, nil)
			if err != nil || response.Id != 100 || len(response.Answer) != 1 {
				t.Fatalf("first=%v err=%v", response, err)
			}
			second := deadlineQuestion()
			second.Id = 200
			response, err = client.Exchange(context.Background(), transport, second, adapter.DNSQueryOptions{}, nil)
			if err != nil || response.Id != 200 || len(response.Answer) != 1 || transport.calls.Load() != 1 {
				t.Fatalf("cached response=%v err=%v calls=%v", response, err, transport.calls.Load())
			}
		})
	}
}

func TestDNSCacheWaitTimeoutDoesNotRemoveLeader(t *testing.T) {
	for _, independent := range []bool{false, true} {
		t.Run(fmt.Sprint(independent), func(t *testing.T) {
			client := NewClient(ClientOptions{Timeout: 60 * time.Millisecond, IndependentCache: independent})
			question := deadlineQuestion().Question[0]
			held := make(chan struct{})
			if independent {
				client.transportCacheLock.Store(question, held)
			} else {
				client.cacheLock.Store(question, held)
			}
			transport := &deadlineTransport{exchange: func(context.Context, *dns.Msg, int32) (*dns.Msg, error) {
				t.Error("expired waiter entered transport")
				return nil, errors.New("unexpected exchange")
			}}
			// Outer deadline only bounds a broken implementation so the regression can terminate.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := time.Now()
			_, err := client.Exchange(ctx, transport, deadlineQuestion(), adapter.DNSQueryOptions{}, nil)
			elapsed := time.Since(started)
			if !errors.Is(err, context.DeadlineExceeded) || elapsed > 250*time.Millisecond {
				t.Errorf("cache wait ignored DNS timeout: elapsed=%s err=%v", elapsed, err)
			}
			var retained chan struct{}
			var ok bool
			if independent {
				retained, ok = client.transportCacheLock.Load(question)
			} else {
				retained, ok = client.cacheLock.Load(question)
			}
			if !ok || retained != held {
				t.Fatal("follower removed another query's lock")
			}
			if transport.calls.Load() != 0 {
				t.Fatal("transport invoked after wait deadline")
			}
		})
	}
}
