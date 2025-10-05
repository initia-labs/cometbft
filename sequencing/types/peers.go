package types

import (
	"sort"
	"sync"
	"time"

	"github.com/cometbft/cometbft/p2p"
)

// PeerSet tracks peers, their advertised height ranges, and a performance grade.
// Peers are selected by grade for specific heights, with grades updated based on
// observed response times.
type PeerSet struct {
	mu    sync.RWMutex
	peers map[p2p.ID]*peerInfo
}

type peerInfo struct {
	baseHeight   int64
	latestHeight int64
	grade        float64
	pending      map[int64]time.Time
	timers       map[int64]*time.Timer
	timeouts     map[int64]struct{}
}

const (
	defaultGrade    = 1.0
	gradeSmoothing  = 0.2
	minLatencyScore = 1e-3
)

// NewPeerSet returns an empty PeerSet.
func NewPeerSet() *PeerSet {
	return &PeerSet{
		peers: make(map[p2p.ID]*peerInfo),
	}
}

// Update records the height range this peer reports.
func (ps *PeerSet) Update(id p2p.ID, baseHeight, latestHeight int64) {
	if baseHeight > latestHeight {
		baseHeight, latestHeight = latestHeight, baseHeight
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()

	info, ok := ps.peers[id]
	if !ok {
		info = &peerInfo{
			grade:   defaultGrade,
			pending: make(map[int64]time.Time),
		}
		ps.peers[id] = info
	}
	info.baseHeight = baseHeight
	info.latestHeight = latestHeight
}

// Remove deletes a peer if present.
func (ps *PeerSet) Remove(id p2p.ID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.peers, id)
}

// Has reports whether the peer exists in the set.
func (ps *PeerSet) Has(id p2p.ID) bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	_, ok := ps.peers[id]
	return ok
}

// TopHeight returns the maximum latest height advertised by any peer.
func (ps *PeerSet) TopHeight() (int64, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	var (
		maxHeight int64
		found     bool
	)
	for _, info := range ps.peers {
		if !found || info.latestHeight > maxHeight {
			maxHeight = info.latestHeight
			found = true
		}
	}
	return maxHeight, found
}

// Peers returns up to k peers, ordered by grade, that can serve the given height.
func (ps *PeerSet) Peers(height int64, k int) []p2p.ID {
	if k <= 0 {
		return nil
	}
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	candidates := make([]p2p.ID, 0, len(ps.peers))
	for id, info := range ps.peers {
		if info.baseHeight <= height && height <= info.latestHeight {
			candidates = append(candidates, id)
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		pi := ps.peers[candidates[i]]
		pj := ps.peers[candidates[j]]
		if pi.grade == pj.grade {
			return candidates[i] < candidates[j]
		}
		return pi.grade > pj.grade
	})

	if len(candidates) > k {
		candidates = candidates[:k]
	}
	return candidates
}

// PeersForRequest returns up to limit peers that are ready to handle a block
// request for the given height. It skips peers with an in-flight request that
// has not yet timed out. Any pending request whose age exceeds the provided
// timeout is cleared and returned in the timedOut slice. The third return
// value reports the total number of peers that can serve this height,
// regardless of whether they currently have a pending request.
func (ps *PeerSet) PeersForRequest(height int64, limit int, now time.Time, timeout time.Duration) ([]p2p.ID, []p2p.ID, int) {
	if limit <= 0 {
		return nil, nil, 0
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()

	candidates := make([]p2p.ID, 0, len(ps.peers))
	timedOut := make([]p2p.ID, 0)
	providers := 0
	activePending := 0

	for id, info := range ps.peers {
		if info.baseHeight > height || height > info.latestHeight {
			continue
		}
		providers++
		_, active := ps.handlePending(id, info, height, now, timeout)
		if ps.consumeTimeout(info, height) {
			timedOut = append(timedOut, id)
		}
		if active {
			activePending++
			continue
		}
		candidates = append(candidates, id)
	}

	if len(candidates) == 0 {
		return nil, timedOut, providers
	}

	sort.Slice(candidates, func(i, j int) bool {
		pi := ps.peers[candidates[i]]
		pj := ps.peers[candidates[j]]
		if pi.grade == pj.grade {
			return candidates[i] < candidates[j]
		}
		return pi.grade > pj.grade
	})

	allowed := limit - activePending
	if allowed <= 0 {
		return nil, timedOut, providers
	}
	if len(candidates) > allowed {
		candidates = candidates[:allowed]
	}

	return candidates, timedOut, providers
}

// HasActiveRequest reports whether any peer currently has a non-expired
// in-flight request for the given height. Any pending requests that have timed
// out are cleared and treated as inactive.
func (ps *PeerSet) HasActiveRequest(height int64, now time.Time, timeout time.Duration) bool {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for id, info := range ps.peers {
		_, active := ps.handlePending(id, info, height, now, timeout)
		if active {
			return true
		}
	}
	return false
}

// RecordRequest notes that a request for the given height was sent to this peer.
// The provided timestamp should typically be time.Now().
func (ps *PeerSet) RecordRequest(id p2p.ID, height int64, ts time.Time, timeout time.Duration) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	info, ok := ps.peers[id]
	if !ok {
		info = &peerInfo{
			grade:   defaultGrade,
			pending: make(map[int64]time.Time),
		}
		ps.peers[id] = info
	} else if info.pending == nil {
		info.pending = make(map[int64]time.Time)
	}
	info.pending[height] = ts
	ps.schedulePendingTimer(id, info, height, timeout)
}

// RecordResponse updates the peer's grade using the response time for the
// specified height. If no matching request is pending, it is ignored.
func (ps *PeerSet) RecordResponse(id p2p.ID, height int64, ts time.Time) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	info, ok := ps.peers[id]
	if !ok {
		return
	}
	start, ok := ps.clearPendingLocked(id, info, height, false)
	if !ok {
		return
	}

	latency := ts.Sub(start).Seconds()
	if latency <= 0 {
		latency = minLatencyScore
	}
	score := 1.0 / latency
	if info.grade == 0 {
		info.grade = score
	} else {
		info.grade = (1-gradeSmoothing)*info.grade + gradeSmoothing*score
	}
}

func (ps *PeerSet) handlePending(id p2p.ID, info *peerInfo, height int64, now time.Time, timeout time.Duration) (expired bool, active bool) {
	if info.pending == nil {
		return false, false
	}
	ts, ok := info.pending[height]
	if !ok {
		return false, false
	}
	if timeout > 0 && now.Sub(ts) >= timeout {
		ps.clearPendingLocked(id, info, height, true)
		return true, false
	}
	return false, true
}

func (ps *PeerSet) clearPendingLocked(id p2p.ID, info *peerInfo, height int64, expired bool) (time.Time, bool) {
	if info.pending == nil {
		return time.Time{}, false
	}
	ts, ok := info.pending[height]
	if !ok {
		return time.Time{}, false
	}
	delete(info.pending, height)
	if info.timers != nil {
		if timer, ok := info.timers[height]; ok {
			timer.Stop()
			delete(info.timers, height)
		}
	}
	if expired && info.grade > 0 {
		info.grade *= 1 - gradeSmoothing
		if info.grade < minLatencyScore {
			info.grade = minLatencyScore
		}
	}
	if expired {
		if info.timeouts == nil {
			info.timeouts = make(map[int64]struct{})
		}
		info.timeouts[height] = struct{}{}
	}
	return ts, true
}

func (ps *PeerSet) consumeTimeout(info *peerInfo, height int64) bool {
	if info.timeouts == nil {
		return false
	}
	if _, ok := info.timeouts[height]; ok {
		delete(info.timeouts, height)
		return true
	}
	return false
}

func (ps *PeerSet) schedulePendingTimer(id p2p.ID, info *peerInfo, height int64, timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	if info.timers == nil {
		info.timers = make(map[int64]*time.Timer)
	}
	if timer, ok := info.timers[height]; ok {
		timer.Stop()
	}
	timer := time.AfterFunc(timeout, func() {
		ps.expireRequest(id, height)
	})
	info.timers[height] = timer
}

func (ps *PeerSet) expireRequest(id p2p.ID, height int64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	info, ok := ps.peers[id]
	if !ok {
		return
	}
	ps.clearPendingLocked(id, info, height, true)
}
