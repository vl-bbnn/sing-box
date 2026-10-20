package fakeip_test

import (
	"context"
	"net/netip"
	"path/filepath"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/dns/transport/fakeip"
	"github.com/sagernet/sing-box/experimental/cachefile"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/service"
)

func openFakeIPCache(t *testing.T, path string) (*cachefile.CacheFile, *fakeip.Store) {
	t.Helper()
	cache := cachefile.New(context.Background(), option.CacheFileOptions{Path: path, StoreFakeIP: true})
	if err := cache.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal(err)
	}
	ctx := service.ContextWith[adapter.CacheFile](context.Background(), cache)
	store := fakeip.NewStore(ctx, logger.NOP(), netip.MustParsePrefix("198.18.0.0/29"), netip.Prefix{})
	if err := store.Start(); err != nil {
		cache.Close()
		t.Fatal(err)
	}
	return cache, store
}

func TestFakeIPCursorSurvivesCacheRestartWithoutStoreClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	cache, store := openFakeIPCache(t, path)
	first, err := store.Create("first.example", false)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a process exit before the FakeIP store's graceful Close hook.
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	cache, store = openFakeIPCache(t, path)
	defer cache.Close()
	second, err := store.Create("second.example", false)
	if err != nil {
		t.Fatal(err)
	}
	if second == first {
		t.Fatalf("reused live FakeIP %v after restart", first)
	}
	if domain, ok := store.Lookup(first); !ok || domain != "first.example" {
		t.Fatalf("lost first binding after restart: %q, %v", domain, ok)
	}
}

func TestFakeIPLegacyStaleCursorSkipsOccupiedAddress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	cache, _ := openFakeIPCache(t, path)
	occupied := netip.MustParseAddr("198.18.0.2")
	if err := cache.FakeIPStore(occupied, "old.example"); err != nil {
		t.Fatal(err)
	}
	if err := cache.FakeIPSaveMetadata(&adapter.FakeIPMetadata{
		Inet4Range:   netip.MustParsePrefix("198.18.0.0/29"),
		Inet4Current: netip.MustParseAddr("198.18.0.1"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	cache, store := openFakeIPCache(t, path)
	defer cache.Close()
	address, err := store.Create("new.example", false)
	if err != nil {
		t.Fatal(err)
	}
	if address == occupied {
		t.Fatalf("stale cursor reused occupied FakeIP %v", occupied)
	}
	if domain, ok := store.Lookup(occupied); !ok || domain != "old.example" {
		t.Fatalf("overwrote old binding: %q, %v", domain, ok)
	}
}

func TestFakeIPLegacyMissingMetadataPreservesBindings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	cache, _ := openFakeIPCache(t, path)
	occupied := netip.MustParseAddr("198.18.0.2")
	if err := cache.FakeIPStore(occupied, "old.example"); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	cache, store := openFakeIPCache(t, path)
	defer cache.Close()
	address, err := store.Create("new.example", false)
	if err != nil {
		t.Fatal(err)
	}
	if address == occupied {
		t.Fatalf("reused occupied FakeIP %v from cache without metadata", occupied)
	}
	if domain, ok := store.Lookup(occupied); !ok || domain != "old.example" {
		t.Fatalf("lost old binding: %q, %v", domain, ok)
	}
}
