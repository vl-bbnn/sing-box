package transport

import (
	"context"
	"errors"
	mDNS "github.com/miekg/dns"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestCollateralDeadlineRetryIsBounded(t *testing.T) {
	var attempts atomic.Int32
	mux := newQueryMultiplexer(queryMultiplexerOptions{
		dial: func(ctx context.Context) (net.Conn, error) {
			attempts.Add(1)
			return nil, errors.Join(context.DeadlineExceeded, errSharedQueryDeadline)
		},
	})
	defer mux.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	message := new(mDNS.Msg)
	message.SetQuestion("bounded.example.com.", mDNS.TypeA)
	_, err := mux.Exchange(ctx, message)
	if !errors.Is(err, errSharedQueryDeadline) || attempts.Load() != 2 {
		t.Fatal("retry must stop after one fresh attempt", attempts.Load(), err)
	}
	if ctx.Err() != nil {
		t.Fatal("retry consumed caller budget unexpectedly")
	}
}

func TestCollateralDeadlineUsesRemainingQueryBudget(t *testing.T) {
	firstWritten := make(chan struct{})
	var dialCount atomic.Int32
	mux := newQueryMultiplexer(queryMultiplexerOptions{
		dial: func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			n := dialCount.Add(1)
			go func() {
				defer server.Close()
				for {
					q, e := ReadMessage(server)
					if e != nil {
						return
					}
					if n == 1 {
						if q.Question[0].Name == "old.example.com." {
							close(firstWritten)
						}
						continue
					}
					response := new(mDNS.Msg)
					response.SetReply(q)
					if WriteMessage(server, q.Id, response) != nil {
						return
					}
				}
			}()
			return client, nil
		},
		write:    func(c net.Conn, m *mDNS.Msg, id uint16) error { return WriteMessage(c, id, m) },
		readNext: func(c net.Conn) (*mDNS.Msg, error) { return ReadMessage(c) },
	})
	defer mux.Close()
	oldCtx, cancelOld := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelOld()
	old := new(mDNS.Msg)
	old.SetQuestion("old.example.com.", mDNS.TypeA)
	done := make(chan error, 1)
	go func() { _, e := mux.Exchange(oldCtx, old); done <- e }()
	<-firstWritten
	freshCtx, cancelFresh := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFresh()
	fresh := new(mDNS.Msg)
	fresh.SetQuestion("fresh.example.com.", mDNS.TypeA)
	response, err := mux.Exchange(freshCtx, fresh)
	if err != nil {
		t.Fatalf("query failed before its own deadline (ctx=%v): %v", freshCtx.Err(), err)
	}
	if response == nil || response.Id != fresh.Id {
		t.Fatal("invalid response")
	}
	if <-done == nil {
		t.Fatal("expired owner query unexpectedly succeeded")
	}
	if dialCount.Load() != 2 {
		t.Fatal("expected one fresh connection", dialCount.Load())
	}
}
