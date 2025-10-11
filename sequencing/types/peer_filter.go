package types

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/cometbft/cometbft/p2p"
)

const (
	peerRelayBloomBits    = 2048
	peerRelayBloomBytes   = peerRelayBloomBits / 8
	peerRelayBloomHashes  = 6
	peerRelayBloomVersion = 1
)

// PeerRelayFilter is a fixed-size bloom filter used to track which peers have
// already received a relayed message. It trades a small false-positive rate for
// bounded message size.
type PeerRelayFilter struct {
	bits []byte
}

// NewPeerRelayFilter constructs an empty bloom filter.
func NewPeerRelayFilter() *PeerRelayFilter {
	return &PeerRelayFilter{bits: make([]byte, peerRelayBloomBytes)}
}

// PeerRelayFilterFromBytes restores a bloom filter from the serialized form used
// on the wire. An empty slice returns nil to preserve "direct reply" semantics.
func PeerRelayFilterFromBytes(data []byte) (*PeerRelayFilter, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if data[0] != peerRelayBloomVersion {
		return nil, fmt.Errorf("unsupported peer relay bloom version %d", data[0])
	}
	payload := data[1:]
	if len(payload) != peerRelayBloomBytes {
		return nil, fmt.Errorf("invalid peer relay bloom size %d (expected %d)", len(payload), peerRelayBloomBytes)
	}
	filter := &PeerRelayFilter{bits: make([]byte, peerRelayBloomBytes)}
	copy(filter.bits, payload)
	return filter, nil
}

// MarshalBinary serializes the filter in a stable format for network transport.
func (f *PeerRelayFilter) MarshalBinary() []byte {
	if f == nil {
		return nil
	}
	out := make([]byte, 1+len(f.bits))
	out[0] = peerRelayBloomVersion
	copy(out[1:], f.bits)
	return out
}

// Clone duplicates the filter so callers can mutate the copy independently.
func (f *PeerRelayFilter) Clone() *PeerRelayFilter {
	if f == nil {
		return nil
	}
	clone := &PeerRelayFilter{bits: make([]byte, len(f.bits))}
	copy(clone.bits, f.bits)
	return clone
}

// Add inserts the peer identifier into the bloom filter.
func (f *PeerRelayFilter) Add(id p2p.ID) {
	if f == nil {
		return
	}
	for _, idx := range bloomIndexes(id) {
		f.bits[idx>>3] |= 1 << (idx & 7)
	}
}

// Contains reports whether the peer identifier is likely present in the bloom
// filter. False positives are possible; false negatives are not.
func (f *PeerRelayFilter) Contains(id p2p.ID) bool {
	if f == nil {
		return false
	}
	for _, idx := range bloomIndexes(id) {
		if f.bits[idx>>3]&(1<<(idx&7)) == 0 {
			return false
		}
	}
	return true
}

// Merge ORs another filter into this one.
func (f *PeerRelayFilter) Merge(other *PeerRelayFilter) {
	if f == nil || other == nil {
		return
	}
	for i := range f.bits {
		f.bits[i] |= other.bits[i]
	}
}

func (f *PeerRelayFilter) BuildOutgoing(ids []p2p.ID) *PeerRelayFilter {
	outgoing := f.Clone()
	if outgoing == nil {
		outgoing = NewPeerRelayFilter()
	}
	for _, id := range ids {
		outgoing.Add(id)
	}
	return outgoing
}

func bloomIndexes(id p2p.ID) []uint16 {
	digest := sha256.Sum256([]byte(id))
	h1 := binary.LittleEndian.Uint64(digest[:8])
	h2 := binary.LittleEndian.Uint64(digest[8:16])
	indexes := make([]uint16, peerRelayBloomHashes)
	for i := 0; i < peerRelayBloomHashes; i++ {
		combined := h1 + uint64(i)*h2
		indexes[i] = uint16(combined % peerRelayBloomBits)
	}
	return indexes
}
