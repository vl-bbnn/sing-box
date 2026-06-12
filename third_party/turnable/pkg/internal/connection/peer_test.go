package connection

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestPeerConnAdaptiveDataKeepsStandbyIdle(t *testing.T) {
	peer := NewPeerConn(context.Background())
	peer.adaptiveEnabled = true
	peer.adaptiveThreshold = 1024
	peer.adaptiveIdle = 100 * time.Millisecond
	defer peer.Close()
	firstLocal, firstRemote := net.Pipe()
	defer firstRemote.Close()
	secondLocal, secondRemote := net.Pipe()
	defer secondRemote.Close()
	if err := peer.AddPeer(firstLocal, nil); err != nil {
		t.Fatal(err)
	}
	if err := peer.AddPeer(secondLocal, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := peer.Write([]byte("light")); err != nil {
		t.Fatal(err)
	}
	if err := firstRemote.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	if _, err := firstRemote.Read(buf); err != nil {
		t.Fatalf("preferred peer did not receive light traffic: %v", err)
	}
	if err := secondRemote.SetReadDeadline(time.Now().Add(25 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := secondRemote.Read(buf); err == nil {
		t.Fatal("standby peer unexpectedly received light traffic")
	}
	if got := peer.Stats().ActiveDataPeers; got != 1 {
		t.Fatalf("active data peers=%d, want 1", got)
	}
}

func TestPeerConnAdaptiveDataActivatesAllPeers(t *testing.T) {
	peer := NewPeerConn(context.Background())
	peer.adaptiveEnabled = true
	peer.adaptiveThreshold = 1
	peer.adaptiveIdle = 100 * time.Millisecond
	defer peer.Close()
	firstLocal, firstRemote := net.Pipe()
	defer firstRemote.Close()
	secondLocal, secondRemote := net.Pipe()
	defer secondRemote.Close()
	if err := peer.AddPeer(firstLocal, nil); err != nil {
		t.Fatal(err)
	}
	if err := peer.AddPeer(secondLocal, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := peer.Write([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Write([]byte("two")); err != nil {
		t.Fatal(err)
	}
	stats := peer.Stats()
	if stats.ActiveDataPeers != 2 || stats.AdaptiveActivations != 1 {
		t.Fatalf("stats=%+v, want both peers activated once", stats)
	}
}

func TestPeerConnAdaptiveDataStaysActiveOnIncomingTraffic(t *testing.T) {
	peer := NewPeerConn(context.Background())
	peer.adaptiveEnabled = true
	peer.adaptiveThreshold = 1
	peer.adaptiveIdle = 25 * time.Millisecond
	defer peer.Close()
	firstLocal, firstRemote := net.Pipe()
	defer firstRemote.Close()
	secondLocal, secondRemote := net.Pipe()
	defer secondRemote.Close()
	if err := peer.AddPeer(firstLocal, nil); err != nil {
		t.Fatal(err)
	}
	if err := peer.AddPeer(secondLocal, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := firstRemote.Write([]byte("download")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	if _, err := peer.Read(buf); err != nil {
		t.Fatal(err)
	}
	if got := peer.Stats().ActiveDataPeers; got != 2 {
		t.Fatalf("active data peers=%d, want 2 after incoming traffic", got)
	}
}
