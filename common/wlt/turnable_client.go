package wlt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"

	turnableconfig "github.com/theairblow/turnable/pkg/config"
	turnableengine "github.com/theairblow/turnable/pkg/engine"
)

type TurnableListenerClientOptions struct {
	Config     string
	ConfigFile string
	Listeners  string
	Logger     func(string, ...any)
}

type TurnableListenerClient struct {
	cancel context.CancelFunc
	client *turnableengine.TurnableClient
	done   <-chan error

	closeOnce sync.Once
	closeErr  error
}

func StartTurnableListenerClient(ctx context.Context, options TurnableListenerClientOptions) (*TurnableListenerClient, error) {
	cfg, err := loadTurnableClientConfig(TurnableOptions{
		Config:     options.Config,
		ConfigFile: options.ConfigFile,
	})
	if err != nil {
		return nil, err
	}
	listenAddrs, listenKeys, err := turnableListenAddrs(cfg, options.Listeners)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	turnableconfig.Options.Interactive = false
	turnableClient := turnableengine.NewTurnableClient(*cfg)
	turnableClient.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := turnableClient.Start(listenAddrs); err != nil {
		cancel()
		return nil, err
	}
	if options.Logger != nil {
		options.Logger("Turnable listener client started routes=%d listen=%s", len(cfg.Routes), strings.Join(listenKeys, ","))
	}

	client := &TurnableListenerClient{
		cancel: cancel,
		client: turnableClient,
	}
	done := make(chan error, 1)
	client.done = done
	go func() {
		<-runCtx.Done()
		done <- client.Close()
	}()
	return client, nil
}

func (c *TurnableListenerClient) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		if c.client != nil {
			if err := c.client.Stop(); err != nil && !strings.Contains(err.Error(), "not running") {
				c.closeErr = err
			}
		}
	})
	return c.closeErr
}

func (c *TurnableListenerClient) Wait() error {
	if c == nil || c.done == nil {
		return nil
	}
	return <-c.done
}

func turnableListenAddrs(cfg *turnableconfig.ClientConfig, spec string) ([]string, []string, error) {
	listeners, err := parseMap(defaultString(spec, "direct=127.0.0.1:12100,eu=127.0.0.1:12101,ru=127.0.0.1:12102"))
	if err != nil {
		return nil, nil, err
	}
	if cfg == nil {
		return nil, nil, errors.New("nil turnable config")
	}
	listenAddrs := make([]string, 0, len(cfg.Routes))
	listenKeys := make([]string, 0, len(cfg.Routes))
	for _, route := range cfg.Routes {
		key := turnableListenerKey(route.RouteID)
		addr := ""
		if route.RouteID != "" {
			addr = listeners[route.RouteID]
		}
		if addr == "" && key != "" {
			addr = listeners[key]
		}
		if addr == "" {
			return nil, nil, fmt.Errorf("no Turnable listener for route %q", route.RouteID)
		}
		listenAddrs = append(listenAddrs, addr)
		if key == "" {
			key = route.RouteID
		}
		listenKeys = append(listenKeys, key)
	}
	return listenAddrs, listenKeys, nil
}

func turnableListenerKey(routeID string) string {
	route := strings.ToLower(routeID)
	switch {
	case strings.Contains(route, "eu"):
		return "eu"
	case strings.Contains(route, "ru"):
		return "ru"
	case strings.Contains(route, "main"), strings.Contains(route, "direct"):
		return "direct"
	default:
		return ""
	}
}

func parseMap(spec string) (map[string]string, error) {
	result := make(map[string]string)
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return result, nil
	}
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		key, value, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("invalid map item %q", item)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			return nil, fmt.Errorf("invalid map item %q", item)
		}
		result[key] = value
	}
	return result, nil
}

func defaultString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
