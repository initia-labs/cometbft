package types

import (
	"testing"

	"github.com/cometbft/cometbft/p2p"
)

func TestBlockBucketPeekAndPopLowestAcrossPeers(t *testing.T) {
	bucket := NewP2PBucket[*ProposedBlock]()

	peer1First := &ProposedBlock{}
	peer1Second := &ProposedBlock{}
	peer2Block := &ProposedBlock{}
	peer3Block := &ProposedBlock{}

	bucket.Add(p2p.ID("peer1"), 10, peer1First)
	bucket.Add(p2p.ID("peer1"), 6, peer1Second)
	bucket.Add(p2p.ID("peer2"), 3, peer2Block)
	bucket.Add(p2p.ID("peer3"), 8, peer3Block)

	if bucket.Len() != 4 {
		t.Fatalf("expected overall length 4, got %d", bucket.Len())
	}

	id, height, value, ok := bucket.PeekLowest()
	if !ok {
		t.Fatalf("expected peek to succeed")
	}
	if id != p2p.ID("peer2") || height != 3 || value != peer2Block {
		t.Fatalf("unexpected peek result: id=%s height=%d value=%v", id, height, value)
	}
	if bucket.Len() != 4 {
		t.Fatalf("peek should not mutate length, got %d", bucket.Len())
	}

	id, height, value, ok = bucket.PopLowest()
	if !ok || id != p2p.ID("peer2") || height != 3 || value != peer2Block {
		t.Fatalf("unexpected first pop: ok=%t id=%s height=%d value=%v", ok, id, height, value)
	}

	id, height, value, ok = bucket.PopLowest()
	if !ok || id != p2p.ID("peer1") || height != 6 || value != peer1Second {
		t.Fatalf("unexpected second pop: ok=%t id=%s height=%d value=%v", ok, id, height, value)
	}

	id, height, value, ok = bucket.PopLowest()
	if !ok || id != p2p.ID("peer3") || height != 8 || value != peer3Block {
		t.Fatalf("unexpected third pop: ok=%t id=%s height=%d value=%v", ok, id, height, value)
	}

	id, height, value, ok = bucket.PopLowest()
	if !ok || id != p2p.ID("peer1") || height != 10 || value != peer1First {
		t.Fatalf("unexpected fourth pop: ok=%t id=%s height=%d value=%v", ok, id, height, value)
	}

	if !bucket.IsEmpty() || bucket.Len() != 0 {
		t.Fatalf("bucket should be empty after removing all entries")
	}
	if bucket.HasPeer(p2p.ID("peer1")) || bucket.HasPeer(p2p.ID("peer2")) || bucket.HasPeer(p2p.ID("peer3")) {
		t.Fatalf("bucket should not report peers after they are drained")
	}

	peer1New := &ProposedBlock{}
	bucket.Add(p2p.ID("peer1"), 15, peer1New)

	id, height, value, ok = bucket.PeekLowest()
	if !ok || id != p2p.ID("peer1") || height != 15 || value != peer1New {
		t.Fatalf("unexpected peek after repopulating: ok=%t id=%s height=%d value=%v", ok, id, height, value)
	}
	id, height, value, ok = bucket.PopLowest()
	if !ok || id != p2p.ID("peer1") || height != 15 || value != peer1New {
		t.Fatalf("unexpected pop after repopulating: ok=%t id=%s height=%d value=%v", ok, id, height, value)
	}
}

func TestBlockBucketRemovePeer(t *testing.T) {
	bucket := NewP2PBucket[*ProposedBlock]()

	bucket.Add(p2p.ID("peer1"), 4, &ProposedBlock{})
	bucket.Add(p2p.ID("peer1"), 9, &ProposedBlock{})
	bucket.Add(p2p.ID("peer2"), 7, &ProposedBlock{})

	if bucket.Len() != 3 {
		t.Fatalf("expected length 3, got %d", bucket.Len())
	}

	removed := bucket.RemovePeer(p2p.ID("peer2"))
	if !removed {
		t.Fatalf("expected peer2 to be removed")
	}
	if bucket.Len() != 2 {
		t.Fatalf("expected length 2 after removing peer2, got %d", bucket.Len())
	}
	if bucket.HasPeer(p2p.ID("peer2")) {
		t.Fatalf("peer2 should no longer be tracked")
	}

	removed = bucket.RemovePeer(p2p.ID("peer-missing"))
	if removed {
		t.Fatalf("expected missing peer removal to return false")
	}

	if bucket.PeerLen(p2p.ID("peer1")) != 2 {
		t.Fatalf("peer1 queue length should remain 2, got %d", bucket.PeerLen(p2p.ID("peer1")))
	}
}

func TestBlockBucketHasHeightAndRemove(t *testing.T) {
	bucket := NewP2PBucket[*ProposedBlock]()

	targetBlock := &ProposedBlock{}
	bucket.Add(p2p.ID("peer1"), 12, &ProposedBlock{})
	bucket.Add(p2p.ID("peer2"), 5, targetBlock)

	if !bucket.HasHeight(5) {
		t.Fatalf("expected bucket to report containing height 5")
	}

	id, value, ok := bucket.Remove(5)
	if !ok {
		t.Fatalf("expected removal of height 5 to succeed")
	}
	if id != p2p.ID("peer2") || value != targetBlock {
		t.Fatalf("unexpected removal result: id=%s value=%v", id, value)
	}
	if bucket.HasHeight(5) {
		t.Fatalf("height 5 should no longer be present")
	}
	if bucket.Len() != 1 {
		t.Fatalf("expected length 1 after removal, got %d", bucket.Len())
	}

	if _, _, _, ok := bucket.PopLowest(); !ok {
		t.Fatalf("expected remaining entry to be popped successfully")
	}

	if _, _, _, ok := bucket.PopLowest(); ok {
		t.Fatalf("expected pop on empty bucket to fail")
	}

	if bucket.HasHeight(500) {
		t.Fatalf("height 500 should never be present")
	}
	if _, _, ok := bucket.Remove(500); ok {
		t.Fatalf("removal of unknown height should fail")
	}
}
