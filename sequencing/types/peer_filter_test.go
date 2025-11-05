package types

import (
	"bytes"
	"testing"

	"github.com/cometbft/cometbft/p2p"
)

func TestPeerRelayFilterAddAndContains(t *testing.T) {
	filter := NewPeerRelayFilter()
	id := p2p.ID("peer-1")
	filter.Add(id)

	if !filter.Contains(id) {
		t.Fatalf("expected filter to contain %q", id)
	}

	if other := p2p.ID("peer-2"); filter.Contains(other) {
		t.Fatalf("did not expect filter to report %q", other)
	}
}

func TestPeerRelayFilterBuildOutgoing(t *testing.T) {
	filter := NewPeerRelayFilter()
	filter.Add(p2p.ID("existing"))

	outgoing := filter.BuildOutgoing([]p2p.ID{"next-1", "next-2"})
	if outgoing == nil {
		t.Fatalf("expected outgoing filter")
	}

	if filter.Contains("next-1") {
		t.Fatalf("original filter should not be modified with new peers")
	}

	for _, id := range []p2p.ID{"existing", "next-1", "next-2"} {
		if !outgoing.Contains(id) {
			t.Fatalf("outgoing filter missing %q", id)
		}
	}
}

func TestPeerRelayFilterMarshalRoundTrip(t *testing.T) {
	filter := NewPeerRelayFilter()
	filter.Add("peer-1")
	filter.Add("peer-2")

	bytes := filter.MarshalBinary()
	if len(bytes) == 0 {
		t.Fatalf("expected non-empty serialization")
	}

	restored, err := PeerRelayFilterFromBytes(bytes)
	if err != nil {
		t.Fatalf("unexpected error round-tripping filter: %v", err)
	}

	for _, id := range []p2p.ID{"peer-1", "peer-2"} {
		if !restored.Contains(id) {
			t.Fatalf("restored filter missing %q", id)
		}
	}
}

func TestPeerRelayFilterFromBytesErrors(t *testing.T) {
	if filter, err := PeerRelayFilterFromBytes(nil); err != nil || filter != nil {
		t.Fatalf("expected nil filter with nil input, got filter=%v err=%v", filter, err)
	}

	data := make([]byte, peerRelayBloomBytes+1)
	data[0] = peerRelayBloomVersion + 1
	if _, err := PeerRelayFilterFromBytes(data); err == nil {
		t.Fatalf("expected version mismatch error")
	}

	data[0] = peerRelayBloomVersion
	if _, err := PeerRelayFilterFromBytes(data[:len(data)-1]); err == nil {
		t.Fatalf("expected size mismatch error")
	}

	valid := append([]byte{peerRelayBloomVersion}, make([]byte, peerRelayBloomBytes)...)
	filter, err := PeerRelayFilterFromBytes(valid)
	if err != nil {
		t.Fatalf("unexpected error for empty but valid filter: %v", err)
	}
	if !bytes.Equal(filter.MarshalBinary(), valid) {
		t.Fatalf("expected stable marshal format")
	}
}

func TestPeerRelayFilterNilReceiver(t *testing.T) {
	var filter *PeerRelayFilter
	filter.Add("ignored") // should not panic
	if filter.Contains("ignored") {
		t.Fatalf("nil filter should never report membership")
	}

	outgoing := filter.BuildOutgoing([]p2p.ID{"out-1"})
	if outgoing == nil {
		t.Fatalf("expected BuildOutgoing to allocate new filter")
	}
	if !outgoing.Contains("out-1") {
		t.Fatalf("outgoing filter missing newly added peer")
	}
}

func TestPeerRelayFilterCloneIndependence(t *testing.T) {
	filter := NewPeerRelayFilter()
	filter.Add("shared")
	clone := filter.Clone()

	if clone == nil {
		t.Fatalf("expected clone to be non-nil")
	}
	if !clone.Contains("shared") {
		t.Fatalf("clone missing pre-existing peer")
	}

	filter.Add("only-original")
	clone.Add("only-clone")

	if filter.Contains("only-clone") {
		t.Fatalf("original filter should not pick up clone additions")
	}
	if clone.Contains("only-original") {
		t.Fatalf("clone should not pick up original additions")
	}
}

func TestPeerRelayFilterMerge(t *testing.T) {
	left := NewPeerRelayFilter()
	right := NewPeerRelayFilter()
	left.Add("left")
	right.Add("right")

	left.Merge(right)
	if !left.Contains("left") || !left.Contains("right") {
		t.Fatalf("merge should combine both filters")
	}
	if right.Contains("left") {
		t.Fatalf("merge should not mutate source filter")
	}
}
