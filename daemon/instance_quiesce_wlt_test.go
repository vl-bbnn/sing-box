//go:build with_wlt

package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing/service"
)

type drainTestManager struct {
	adapter.ServiceManager
	services []adapter.Service
}

func (m *drainTestManager) Services() []adapter.Service { return m.services }

type drainTestService struct {
	adapter.Service
	kind  string
	drain func(context.Context) error
}

func (s *drainTestService) Type() string                                  { return s.kind }
func (s *drainTestService) QuiescePendingDials(ctx context.Context) error { return s.drain(ctx) }

func TestWLTShutdownOnlyDrainsWLTUnderOneDeadline(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	var deadlines []time.Time
	actual := errors.New("drain failure")
	makeWLT := func(result error) *drainTestService {
		return &drainTestService{kind: C.TypeWLT, drain: func(ctx context.Context) error {
			if parent.Err() != nil {
				t.Fatal("parent canceled before drain")
			}
			d, ok := ctx.Deadline()
			if !ok {
				t.Fatal("unbounded shutdown drain")
			}
			deadlines = append(deadlines, d)
			return result
		}}
	}
	manager := &drainTestManager{services: []adapter.Service{
		makeWLT(actual),
		&drainTestService{kind: "ordinary", drain: func(context.Context) error { t.Fatal("ordinary service changed"); return nil }},
		makeWLT(nil),
	}}
	ctx := service.ContextWith[adapter.ServiceManager](parent, manager)
	started := time.Now()
	if err := quiesceWLTServices(ctx); !errors.Is(err, actual) {
		t.Fatalf("drain failure lost: %v", err)
	}
	if len(deadlines) != 2 || deadlines[0] != deadlines[1] || deadlines[0].Sub(started) > wltShutdownDrainTimeout+time.Millisecond {
		t.Fatal("per-service fresh budget or incomplete drain", deadlines)
	}
	if parent.Err() != nil {
		t.Fatal("drain owns parent cancellation")
	}
}

func TestWLTShutdownHonorsEarlierParentDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	manager := &drainTestManager{services: []adapter.Service{&drainTestService{kind: C.TypeWLT, drain: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}}}
	started := time.Now()
	err := quiesceWLTServices(service.ContextWith[adapter.ServiceManager](parent, manager))
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 300*time.Millisecond {
		t.Fatal("deadline lost", err)
	}
}

func TestWLTShutdownWithoutServicesRemainsImmediate(t *testing.T) {
	if err := quiesceWLTServices(context.Background()); err != nil {
		t.Fatal(err)
	}
}
