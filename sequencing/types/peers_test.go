package types

import (
	"testing"
	"time"

	"github.com/cometbft/cometbft/p2p"
)

func TestPeerSetUpdateHasAndRemove(t *testing.T) {
	ps := NewPeerSet()
	ps.Update(p2p.ID("peer1"), 15, 5) // intentionally reversed
	if !ps.Has("peer1") {
		t.Fatalf("expected peer1 to be present")
	}

	info := ps.peers[p2p.ID("peer1")]
	if info.baseHeight != 5 || info.latestHeight != 15 {
		t.Fatalf("expected normalized heights 5-15, got %d-%d", info.baseHeight, info.latestHeight)
	}

	ps.Remove("peer1")
	if ps.Has("peer1") {
		t.Fatalf("expected peer1 to be removed")
	}
}

func TestPeerSetTopHeight(t *testing.T) {
	ps := NewPeerSet()
	if _, ok := ps.TopHeight(); ok {
		t.Fatalf("expected no top height for empty set")
	}

	ps.Update("peer1", 1, 10)
	ps.Update("peer2", 1, 12)
	top, ok := ps.TopHeight()
	if !ok || top != 12 {
		t.Fatalf("expected top height 12, got (%d, %v)", top, ok)
	}
}

func TestPeerSetPeersOrdersByGrade(t *testing.T) {
	ps := NewPeerSet()
	for _, id := range []p2p.ID{"slow", "medium", "fast"} {
		ps.Update(id, 1, 100)
	}

	now := time.Now()
	ps.RecordRequest("fast", 10, now.Add(-1*time.Millisecond), 0)
	ps.RecordRequest("medium", 10, now.Add(-50*time.Millisecond), 0)
	ps.RecordRequest("slow", 10, now.Add(-200*time.Millisecond), 0)

	ps.RecordResponse("fast", 10, now)
	ps.RecordResponse("medium", 10, now)
	ps.RecordResponse("slow", 10, now)

	peers := ps.Peers(10, 3)
	expected := []p2p.ID{"fast", "medium", "slow"}
	for i, id := range expected {
		if i >= len(peers) || peers[i] != id {
			t.Fatalf("expected peers ordered %v, got %v", expected, peers)
		}
	}
}

func TestPeerSetPeersLimitZero(t *testing.T) {
	ps := NewPeerSet()
	ps.Update("peer1", 1, 10)
	if peers := ps.Peers(5, 0); peers != nil {
		t.Fatalf("expected nil slice when limit is zero, got %v", peers)
	}
}

func TestPeerSetTimeoutPenalizesGrade(t *testing.T) {
	ps := NewPeerSet()
	ps.Update("peer1", 1, 100)

	now := time.Now()
	ps.RecordRequest("peer1", 10, now.Add(-10*time.Second), 0)
	ps.PeersForRequest(10, 1, now, 5*time.Second) // should time out and penalize

	info := ps.peers["peer1"]
	if info.grade >= defaultGrade {
		t.Fatalf("expected grade to be penalized below default, got %f", info.grade)
	}
	if info.grade < minLatencyScore {
		t.Fatalf("grade should not fall below min latency score, got %f", info.grade)
	}
}

func TestPeerSetRecordRequestCreatesPeer(t *testing.T) {
	ps := NewPeerSet()
	now := time.Now()
	ps.RecordRequest("new-peer", 5, now, 0)
	if !ps.Has("new-peer") {
		t.Fatalf("expected record request to create peer entry")
	}
	if _, active := ps.handlePending("new-peer", ps.peers["new-peer"], 5, now, 0); !active {
		t.Fatalf("expected pending request to be active")
	}
}

func TestPeerSetRecordResponseWithoutPending(t *testing.T) {
	ps := NewPeerSet()
	now := time.Now()

	ps.Update("peer", 1, 10)
	ps.RecordResponse("peer", 5, now)
	info := ps.peers["peer"]
	if info.grade != defaultGrade {
		t.Fatalf("expected grade to remain default when no pending request, got %f", info.grade)
	}
}

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
