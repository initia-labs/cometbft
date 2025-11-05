package types

import (
	"sync"
	"time"
)

// BlockRequestTracker bounds concurrent block requests and expires stale ones.
type BlockRequestTracker struct {
	mu          sync.Mutex
	inflight    map[int64]time.Time
	maxInflight int
	ttl         time.Duration
}

// NewBlockRequestTracker returns a tracker with the provided capacity and ttl.
func NewBlockRequestTracker(maxInflight int, lifetime time.Duration) *BlockRequestTracker {
	return &BlockRequestTracker{
		inflight:    make(map[int64]time.Time),
		maxInflight: maxInflight,
		ttl:         lifetime,
	}
}

// TryReserve attempts to reserve an in-flight slot for the height.
func (t *BlockRequestTracker) TryReserve(height int64, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.gcLocked(now)

	if _, exists := t.inflight[height]; exists {
		return false
	}
	if len(t.inflight) >= t.maxInflight {
		return false
	}

	t.inflight[height] = now
	return true
}

// Release frees the reservation for the height.
func (t *BlockRequestTracker) Release(height int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.inflight, height)
}

// Active returns the number of non-expired reservations.
func (t *BlockRequestTracker) Active(now time.Time) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.gcLocked(now)
	return len(t.inflight)
}

// GC removes reservations older than the tracker ttl.
func (t *BlockRequestTracker) GC(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.gcLocked(now)
}

func (t *BlockRequestTracker) gcLocked(now time.Time) {
	if t.ttl <= 0 {
		return
	}
	for height, ts := range t.inflight {
		if now.Sub(ts) > t.ttl {
			delete(t.inflight, height)
		}
	}
}
