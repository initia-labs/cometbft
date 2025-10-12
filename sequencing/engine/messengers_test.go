package engine

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/cometbft/cometbft/crypto/tmhash"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	p2pmock "github.com/cometbft/cometbft/p2p/mock"
	seqproto "github.com/cometbft/cometbft/proto/tendermint/sequencing"
	"github.com/cometbft/cometbft/sequencing/types"
	comettypes "github.com/cometbft/cometbft/types"
)

type fakeReactor struct {
	mu     sync.RWMutex
	peers  map[p2p.ID]p2p.Peer
	selfID p2p.ID

	txsAvailable chan struct{}
}

func newFakeReactor() *fakeReactor {
	return &fakeReactor{
		peers:        make(map[p2p.ID]p2p.Peer),
		selfID:       p2p.ID("self"),
		txsAvailable: make(chan struct{}, 1),
	}
}

func (r *fakeReactor) MempoolSize() int {
	return 0
}

func (r *fakeReactor) TxsAvailable() <-chan struct{} {
	return r.txsAvailable
}

func (r *fakeReactor) Peers() []p2p.Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	peers := make([]p2p.Peer, 0, len(r.peers))
	for _, p := range r.peers {
		peers = append(peers, p)
	}
	return peers
}

func (r *fakeReactor) SelfID() p2p.ID { return r.selfID }

func (r *fakeReactor) PeerIDs() []p2p.ID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]p2p.ID, 0, len(r.peers))
	for id := range r.peers {
		ids = append(ids, id)
	}
	return ids
}

func (r *fakeReactor) Peer(id p2p.ID) p2p.Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.peers[id]
}

func (r *fakeReactor) addPeer(peer p2p.Peer) {
	r.mu.Lock()
	r.peers[peer.ID()] = peer
	r.mu.Unlock()
}

type recordingPeer struct {
	*p2pmock.Peer
	mu      sync.Mutex
	heights []int64
}

func newRecordingPeer() *recordingPeer {
	return &recordingPeer{Peer: p2pmock.NewPeer(nil)}
}

func (rp *recordingPeer) Send(env p2p.Envelope) bool {
	rp.mu.Lock()
	if msg, ok := env.Message.(*seqproto.Message); ok {
		if br := msg.GetBlockRequest(); br != nil {
			rp.heights = append(rp.heights, br.Height)
		}
	}
	rp.mu.Unlock()
	return rp.Peer.Send(env)
}

func (rp *recordingPeer) RequestedHeights() []int64 {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return append([]int64(nil), rp.heights...)
}

type trackingPeer struct {
	*p2pmock.Peer
	mu        sync.Mutex
	envelopes []p2p.Envelope
	tryOK     bool
}

func newTrackingPeer(tryOK bool) *trackingPeer {
	return &trackingPeer{
		Peer:  p2pmock.NewPeer(nil),
		tryOK: tryOK,
	}
}

func (tp *trackingPeer) TrySend(env p2p.Envelope) bool {
	tp.mu.Lock()
	tp.envelopes = append(tp.envelopes, env)
	tp.mu.Unlock()
	return tp.tryOK
}

func (tp *trackingPeer) SentEnvelopes() []p2p.Envelope {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	out := make([]p2p.Envelope, len(tp.envelopes))
	copy(out, tp.envelopes)
	return out
}

func newTestEngine() *Engine {
	reactor := newFakeReactor()
	return &Engine{
		logger:        log.NewNopLogger(),
		reactor:       reactor,
		peerSet:       types.NewPeerSet(),
		badPeers:      &sync.Map{},
		blockBucket:   types.NewP2PBucket[*types.ProposedBlock](),
		commitBucket:  types.NewP2PBucket[*types.AttestorCommit](),
		requestWindow: types.NewBlockRequestTracker(maxRequestsPerHeight*maxFutureBlocks, time.Second),
		metrics:       NopMetrics(),
	}
}

func TestRequestFutureBlocksRespectsLimit(t *testing.T) {
	e := newTestEngine()
	peer := newRecordingPeer()
	defer peer.Stop() //nolint:errcheck // mock peer

	peerID := peer.ID()
	e.reactor.(*fakeReactor).addPeer(peer)
	e.peerSet.Update(peerID, 1, 500)

	e.requestFutureBlocks(10)

	heights := peer.RequestedHeights()
	if len(heights) != maxFutureBlocks {
		t.Fatalf("expected %d requests, got %d", maxFutureBlocks, len(heights))
	}
	for i, h := range heights {
		expected := int64(11 + i)
		if h != expected {
			t.Fatalf("expected height %d at index %d, got %d", expected, i, h)
		}
	}

	if active := e.requestWindow.Active(time.Now()); active != maxFutureBlocks {
		t.Fatalf("expected %d active reservations, got %d", maxFutureBlocks, active)
	}
}

func TestRequestFutureBlocksSkipsMissingPeers(t *testing.T) {
	e := newTestEngine()
	badID := p2p.ID("bad-peer")
	e.peerSet.Update(badID, 1, 100)

	e.requestFutureBlocks(10)

	if e.requestWindow.Active(time.Now()) != 0 {
		t.Fatalf("expected reservations to be released when no peers send")
	}
	if e.peerSet.Has(badID) {
		t.Fatalf("expected bad peer to be removed after failed request")
	}

	good := newRecordingPeer()
	defer good.Stop() //nolint:errcheck // mock peer
	goodID := good.ID()
	e.reactor.(*fakeReactor).addPeer(good)
	e.peerSet.Update(goodID, 1, 100)

	e.requestFutureBlocks(10)

	heights := good.RequestedHeights()
	if len(heights) == 0 {
		t.Fatalf("expected good peer to receive requests after bad peer removal")
	}
	for _, h := range heights {
		if h < 11 || h > 10+maxFutureBlocks {
			t.Fatalf("unexpected request height %d", h)
		}
	}
}

func TestBroadcastAttestorCommitSkipsFilteredPeers(t *testing.T) {
	e := newTestEngine()
	reactor := e.reactor.(*fakeReactor)

	skipPeer := newTrackingPeer(true)
	defer skipPeer.Stop() //nolint:errcheck // mock peer
	targetPeer := newTrackingPeer(true)
	defer targetPeer.Stop() //nolint:errcheck // mock peer
	extraPeer := newTrackingPeer(true)
	defer extraPeer.Stop() //nolint:errcheck // mock peer

	reactor.addPeer(skipPeer)
	reactor.addPeer(targetPeer)
	reactor.addPeer(extraPeer)

	filter := types.NewPeerRelayFilter()
	filter.Add(skipPeer.ID())
	originalBloom := filter.MarshalBinary()

	hash := tmhash.New()
	hash.Write([]byte("non-empty-hash"))
	attestorCommit := &types.AttestorCommit{
		Commit: &comettypes.ExtendedCommit{
			Height: 7,
			ExtendedSignatures: []comettypes.ExtendedCommitSig{
				comettypes.NewExtendedCommitSigAbsent(),
			},
			BlockID: comettypes.BlockID{Hash: hash.Sum(nil)},
		},
		PeerFilter: filter,
	}

	e.broadcastAttestorCommit(attestorCommit)

	if sent := skipPeer.SentEnvelopes(); len(sent) != 0 {
		t.Fatalf("expected no envelopes sent to filtered peer, got %d", len(sent))
	}

	sent := targetPeer.SentEnvelopes()
	if len(sent) != 1 {
		t.Fatalf("expected 1 envelope for target peer, got %d", len(sent))
	}

	msg, ok := sent[0].Message.(*seqproto.Message)
	if !ok {
		t.Fatalf("expected sequencing proto message, got %T", sent[0].Message)
	}
	protoResp := msg.GetBlockResponse()
	if protoResp == nil {
		t.Fatalf("expected block response payload")
	}

	resp, err := types.BlockResponseFromProto(protoResp)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if resp.AttesterCommit == nil {
		t.Fatalf("expected attestor commit in response")
	}
	if resp.AttesterCommit.Commit == nil || resp.AttesterCommit.Commit.Height != 7 {
		t.Fatalf("unexpected attestor commit payload: %#v", resp.AttesterCommit.Commit)
	}
	if resp.PeerFilter == nil {
		t.Fatalf("expected outgoing peer filter")
	}
	if !resp.PeerFilter.Contains(targetPeer.ID()) {
		t.Fatalf("expected outgoing filter to contain target peer")
	}
	if !resp.PeerFilter.Contains(skipPeer.ID()) {
		t.Fatalf("expected outgoing filter to retain original peer")
	}
	if !resp.PeerFilter.Contains(reactor.SelfID()) {
		t.Fatalf("expected outgoing filter to contain self id")
	}
	for _, id := range reactor.PeerIDs() {
		if !resp.PeerFilter.Contains(id) {
			t.Fatalf("expected outgoing filter to contain peer %q", id)
		}
	}

	if !bytes.Equal(originalBloom, attestorCommit.PeerFilter.MarshalBinary()) {
		t.Fatalf("expected original filter to remain unchanged")
	}
}

func TestBroadcastProposedBlockSkipsFilteredPeers(t *testing.T) {
	e := newTestEngine()
	reactor := e.reactor.(*fakeReactor)

	skipPeer := newTrackingPeer(true)
	defer skipPeer.Stop() //nolint:errcheck // mock peer
	targetPeer := newTrackingPeer(true)
	defer targetPeer.Stop() //nolint:errcheck // mock peer
	extraPeer := newTrackingPeer(true)
	defer extraPeer.Stop() //nolint:errcheck // mock peer

	reactor.addPeer(skipPeer)
	reactor.addPeer(targetPeer)
	reactor.addPeer(extraPeer)
	filter := types.NewPeerRelayFilter()
	filter.Add(skipPeer.ID())
	originalBloom := filter.MarshalBinary()

	hash := tmhash.New()
	hash.Write([]byte("non-empty-hash"))
	lastCommit := &comettypes.Commit{
		Height:  1,
		Round:   0,
		BlockID: comettypes.BlockID{Hash: hash.Sum(nil)},
		Signatures: []comettypes.CommitSig{
			comettypes.NewCommitSigAbsent(),
		},
	}
	block := comettypes.MakeBlock(2, nil, lastCommit, nil)
	block.Header.ProposerAddress = make([]byte, 20)

	proposedBlock := &types.ProposedBlock{
		Block: block,
		Commit: &comettypes.ExtendedCommit{
			Height:  2,
			BlockID: comettypes.BlockID{Hash: hash.Sum(nil)},
			ExtendedSignatures: []comettypes.ExtendedCommitSig{
				comettypes.NewExtendedCommitSigAbsent(),
			},
		},
		PeerFilter: filter,
	}

	e.broadcastProposedBlock(proposedBlock)

	if sent := skipPeer.SentEnvelopes(); len(sent) != 0 {
		t.Fatalf("expected no envelopes sent to filtered peer, got %d", len(sent))
	}

	sent := targetPeer.SentEnvelopes()
	if len(sent) != 1 {
		t.Fatalf("expected 1 envelope for target peer, got %d", len(sent))
	}

	msg, ok := sent[0].Message.(*seqproto.Message)
	if !ok {
		t.Fatalf("expected sequencing proto message, got %T", sent[0].Message)
	}
	protoResp := msg.GetBlockResponse()
	if protoResp == nil {
		t.Fatalf("expected block response payload")
	}

	resp, err := types.BlockResponseFromProto(protoResp)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if resp.ProposedBlock == nil {
		t.Fatalf("expected proposed block in response")
	}
	if resp.ProposedBlock.Block == nil || resp.ProposedBlock.Block.Header.Height != 2 {
		t.Fatalf("unexpected proposed block payload: %#v", resp.ProposedBlock.Block)
	}
	if resp.PeerFilter == nil {
		t.Fatalf("expected outgoing peer filter")
	}
	if !resp.PeerFilter.Contains(targetPeer.ID()) {
		t.Fatalf("expected outgoing filter to contain target peer")
	}
	if !resp.PeerFilter.Contains(skipPeer.ID()) {
		t.Fatalf("expected outgoing filter to retain original peer")
	}
	if !resp.PeerFilter.Contains(reactor.SelfID()) {
		t.Fatalf("expected outgoing filter to contain self id")
	}
	for _, id := range reactor.PeerIDs() {
		if !resp.PeerFilter.Contains(id) {
			t.Fatalf("expected outgoing filter to contain peer %q", id)
		}
	}

	if !bytes.Equal(originalBloom, proposedBlock.PeerFilter.MarshalBinary()) {
		t.Fatalf("expected original filter to remain unchanged")
	}
}
