package types

import (
	"testing"
	"time"
)

func TestBlockRequestTrackerReserveAndRelease(t *testing.T) {
	tracker := NewBlockRequestTracker(2, time.Second)
	now := time.Now()

	if !tracker.TryReserve(1, now) {
		t.Fatalf("expected first reserve to succeed")
	}
	if tracker.Active(now) != 1 {
		t.Fatalf("expected active count to be 1, got %d", tracker.Active(now))
	}

	if tracker.TryReserve(1, now) {
		t.Fatalf("expected duplicate reserve to fail")
	}

	tracker.Release(1)
	if tracker.Active(now) != 0 {
		t.Fatalf("expected active count to return to 0, got %d", tracker.Active(now))
	}
}

func TestBlockRequestTrackerMaxInflight(t *testing.T) {
	tracker := NewBlockRequestTracker(2, time.Second)
	now := time.Now()

	if !tracker.TryReserve(1, now) || !tracker.TryReserve(2, now) {
		t.Fatalf("expected first two reserves to succeed")
	}
	if tracker.TryReserve(3, now) {
		t.Fatalf("expected third reserve to fail due to max inflight")
	}
}

func TestBlockRequestTrackerTTLExpiry(t *testing.T) {
	tracker := NewBlockRequestTracker(2, 5*time.Millisecond)
	now := time.Now()
	if !tracker.TryReserve(1, now) {
		t.Fatalf("expected reserve to succeed")
	}

	time.Sleep(15 * time.Millisecond)
	if tracker.Active(time.Now()) != 0 {
		t.Fatalf("expected active to clear after TTL")
	}
	if !tracker.TryReserve(1, time.Now()) {
		t.Fatalf("expected reserve to succeed after TTL cleared")
	}
}
