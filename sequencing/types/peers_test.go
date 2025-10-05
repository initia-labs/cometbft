package types

import (
	"testing"
	"time"

	"github.com/cometbft/cometbft/p2p"
)

func TestPeersForRequestRespectsLimit(t *testing.T) {
	ps := NewPeerSet()
	ps.Update(p2p.ID("peer1"), 1, 100)
	ps.Update(p2p.ID("peer2"), 1, 100)
	ps.Update(p2p.ID("peer3"), 1, 100)

	now := time.Now()
	selected, timedOut, providers := ps.PeersForRequest(10, 2, now, 5*time.Second)
	if providers != 3 {
		t.Fatalf("expected 3 providers, got %d", providers)
	}
	if len(timedOut) != 0 {
		t.Fatalf("expected no timeouts, got %d", len(timedOut))
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(selected))
	}
	if selected[0] != p2p.ID("peer1") || selected[1] != p2p.ID("peer2") {
		t.Fatalf("unexpected peers selected: %v", selected)
	}
}

func TestPeersForRequestSkipsActivePending(t *testing.T) {
	ps := NewPeerSet()
	ps.Update(p2p.ID("peer1"), 1, 100)
	ps.Update(p2p.ID("peer2"), 1, 100)
	ps.Update(p2p.ID("peer3"), 1, 100)

	now := time.Now()
	ps.RecordRequest(p2p.ID("peer1"), 10, now.Add(-time.Second), 0)

	selected, timedOut, providers := ps.PeersForRequest(10, 2, now, 5*time.Second)
	if providers != 3 {
		t.Fatalf("expected 3 providers, got %d", providers)
	}
	if len(timedOut) != 0 {
		t.Fatalf("expected no timeouts, got %d", len(timedOut))
	}
	if len(selected) != 1 {
		t.Fatalf("expected 1 peer selected, got %d", len(selected))
	}
	if selected[0] != p2p.ID("peer2") {
		t.Fatalf("expected peer2 selected, got %v", selected)
	}
}

func TestPeersForRequestHandlesTimeout(t *testing.T) {
	ps := NewPeerSet()
	ps.Update(p2p.ID("peer1"), 1, 100)
	ps.Update(p2p.ID("peer2"), 1, 100)
	ps.Update(p2p.ID("peer3"), 1, 100)

	now := time.Now()
	ps.RecordRequest(p2p.ID("peer1"), 10, now.Add(-10*time.Second), 0)

	selected, timedOut, providers := ps.PeersForRequest(10, 2, now, 5*time.Second)
	if providers != 3 {
		t.Fatalf("expected 3 providers, got %d", providers)
	}
	if len(timedOut) != 1 || timedOut[0] != p2p.ID("peer1") {
		t.Fatalf("expected peer1 to time out, got %v", timedOut)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 peers selected, got %d", len(selected))
	}
	if selected[0] != p2p.ID("peer2") || selected[1] != p2p.ID("peer3") {
		t.Fatalf("unexpected peers selected: %v", selected)
	}
}

func TestPeersForRequestNoProviders(t *testing.T) {
	ps := NewPeerSet()
	ps.Update(p2p.ID("peer1"), 1, 5)

	selected, timedOut, providers := ps.PeersForRequest(10, 2, time.Now(), 5*time.Second)
	if providers != 0 {
		t.Fatalf("expected 0 providers, got %d", providers)
	}
	if len(timedOut) != 0 {
		t.Fatalf("expected no timeouts, got %d", len(timedOut))
	}
	if len(selected) != 0 {
		t.Fatalf("expected no peers selected, got %d", len(selected))
	}
}

func TestHasActiveRequest(t *testing.T) {
	ps := NewPeerSet()
	ps.Update(p2p.ID("peer1"), 1, 100)

	now := time.Now()
	ps.RecordRequest(p2p.ID("peer1"), 20, now, 0)
	if !ps.HasActiveRequest(20, now, 5*time.Second) {
		t.Fatalf("expected active request to be reported")
	}
}

func TestHasActiveRequestTimeout(t *testing.T) {
	ps := NewPeerSet()
	ps.Update(p2p.ID("peer1"), 1, 100)

	now := time.Now()
	ps.RecordRequest(p2p.ID("peer1"), 20, now.Add(-10*time.Second), 0)
	if ps.HasActiveRequest(20, now, 5*time.Second) {
		t.Fatalf("expected timed-out request to be cleared")
	}
	selected, _, providers := ps.PeersForRequest(20, 1, now, 5*time.Second)
	if providers != 1 {
		t.Fatalf("expected 1 provider, got %d", providers)
	}
	if len(selected) != 1 || selected[0] != p2p.ID("peer1") {
		t.Fatalf("expected peer1 to be available, got %v", selected)
	}
}

func TestRecordRequestTimerExpires(t *testing.T) {
	ps := NewPeerSet()
	ps.Update(p2p.ID("peer1"), 1, 100)

	ps.RecordRequest(p2p.ID("peer1"), 20, time.Now(), 25*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	if ps.HasActiveRequest(20, time.Now(), 5*time.Second) {
		t.Fatalf("expected timer to clear in-flight request")
	}
	_, timedOut, providers := ps.PeersForRequest(20, 1, time.Now(), 5*time.Second)
	if providers != 1 {
		t.Fatalf("expected 1 provider, got %d", providers)
	}
	if len(timedOut) != 1 || timedOut[0] != p2p.ID("peer1") {
		t.Fatalf("expected timeout to be reported for peer1, got %v", timedOut)
	}
}
