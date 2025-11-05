package types

import "sync"

// PeerHeight tracks the latest height known for a peer. It satisfies the
// evidence reactor's PeerState interface via GetHeight.
type PeerHeight struct {
	mu     sync.RWMutex
	height int64
}

// NewPeerHeight returns a peer height tracker initialized to zero.
func NewPeerHeight() *PeerHeight {
	return &PeerHeight{}
}

// GetHeight returns the most recently recorded height for the peer.
func (ps *PeerHeight) GetHeight() int64 {
	ps.mu.RLock()
	h := ps.height
	ps.mu.RUnlock()
	return h
}

// SetHeight updates the recorded height for the peer.
func (ps *PeerHeight) SetHeight(height int64) {
	ps.mu.Lock()
	ps.height = height
	ps.mu.Unlock()
}
