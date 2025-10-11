package engine

import (
	"time"

	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/sequencing/types"
)

const (
	maxFutureBlocks      = 20
	maxRequestsPerHeight = 2
	blockRequestTimeout  = 5 * time.Second
)

// requestFutureBlocks requests missing blocks so that we keep up to
// maxFutureBlocks future blocks buffered when peers can provide them.
func (e *Engine) requestFutureBlocks(stateHeight int64) {
	if e.reactor == nil {
		return
	}

	now := time.Now()
	e.requestWindow.GC(now)

	topHeight, found := e.peerSet.TopHeight()
	if !found {
		// no peers found, we're not syncing
		e.metrics.Syncing.Set(0)
		return
	}

	if stateHeight+CatchUpThreshold < topHeight {
		e.metrics.Syncing.Set(1)
	} else {
		e.metrics.Syncing.Set(0)
	}

	upperBound := min(topHeight, stateHeight+maxFutureBlocks)
	if upperBound <= stateHeight {
		return
	}

	buffered := e.countBufferedHeights(stateHeight+1, upperBound)
	buffered += e.requestWindow.Active(now)
	if buffered >= maxFutureBlocks {
		return
	}

	for h := stateHeight + 1; h <= upperBound && buffered < maxFutureBlocks; h++ {
		if e.blockBucket.HasHeight(h) {
			e.requestWindow.Release(h)
			continue
		}
		if !e.requestWindow.TryReserve(h, now) {
			continue
		}
		if e.peerSet.HasActiveRequest(h, now, blockRequestTimeout) {
			e.requestWindow.Release(h)
			continue
		}
		if !e.requestBlock(h) {
			e.requestWindow.Release(h)
			continue
		}

		buffered++
		if buffered < maxFutureBlocks {
			// small sleep to avoid flooding the network
			time.Sleep(2 * time.Millisecond)
		}
	}
}

// requestBlock requests the block at height h from up to maxRequestsPerHeight peers.
func (e *Engine) requestBlock(h int64) bool {
	if e.reactor == nil {
		return false
	}

	now := time.Now()
	if e.peerSet.HasActiveRequest(h, now, blockRequestTimeout) {
		return false
	}
	pids, timedOut, providers := e.peerSet.PeersForRequest(h, maxRequestsPerHeight, now, blockRequestTimeout)
	for _, pid := range timedOut {
		e.logger.Debug("block request timed out", "peer", pid, "height", h)
	}
	if providers == 0 {
		e.logger.Error("no peers can serve height", "height", h)
		return false
	}
	if len(pids) == 0 {
		return false
	}

	sent := false
	for _, pid := range pids {
		peer := e.reactor.Peer(pid)
		if peer == nil {
			e.peerSet.Remove(pid)
			continue
		}

		peer.Send(p2p.Envelope{
			Message:   types.MsgToProto(&types.BlockRequest{Height: h}),
			ChannelID: types.SyncChannel,
		})
		e.peerSet.RecordRequest(pid, h, now, blockRequestTimeout)
		sent = true
	}

	return sent
}

func (e *Engine) countBufferedHeights(start, end int64) int {
	count := 0
	for h := start; h <= end; h++ {
		if e.blockBucket.HasHeight(h) {
			count++
		}
	}
	return count
}

// broadcastStatus sends the current status (base and latest heights) to all connected peers.
func (e *Engine) broadcastStatus() {
	if e.reactor == nil {
		return
	}

	bh := e.blockStore.Base()
	lh := e.blockStore.Height()

	for _, peer := range e.reactor.Peers() {
		env := p2p.Envelope{
			Message:   types.MsgToProto(&types.StatusUpdate{BaseHeight: bh, LastHeight: lh}),
			ChannelID: types.SyncChannel,
		}
		if peer.TrySend(env) {
			e.metrics.StatusBroadcasts.Add(1)
		} else {
			e.metrics.StatusBroadcastFailures.Add(1)
			e.logger.Debug("status broadcast backpressured", "peer", peer.ID())
		}
	}
}

// broadcastAttestorCommit sends the attestor commit to all connected peers except the
// provided peer IDs. It assumes the caller already ensured that broadcasting is warranted.
func (e *Engine) broadcastAttestorCommit(ac *types.AttestorCommit) {
	if ac == nil {
		return
	}
	if e.reactor == nil {
		return
	}

	existing := ac.PeerFilter
	outgoing := existing.BuildOutgoing(append(e.reactor.PeerIDs(), e.reactor.SelfID()))
	for _, peer := range e.reactor.Peers() {
		if existing != nil && existing.Contains(peer.ID()) {
			continue
		}
		env := p2p.Envelope{
			Message: types.MsgToProto(&types.BlockResponse{
				ProposedBlock:  nil,
				AttesterCommit: ac,
				PeerFilter:     outgoing,
			}),
			ChannelID: types.AttestChannel,
		}
		if peer.TrySend(env) {
			e.metrics.AttestBroadcasts.Add(1)
		} else {
			e.metrics.AttestBroadcastFailures.Add(1)
			e.logger.Debug("attestor commit broadcast backpressured", "peer", peer.ID(), "height", ac.Commit.Height)
		}
	}
}

// broadcastProposedBlock sends the proposed block to all connected peers.
func (e *Engine) broadcastProposedBlock(pb *types.ProposedBlock) {
	if pb == nil {
		return
	}
	if e.reactor == nil {
		return
	}

	peers := e.reactor.Peers()
	existing := pb.PeerFilter
	outgoing := existing.BuildOutgoing(append(e.reactor.PeerIDs(), e.reactor.SelfID()))
	for _, peer := range peers {
		if existing != nil && existing.Contains(peer.ID()) {
			continue
		}
		env := p2p.Envelope{
			Message: types.MsgToProto(&types.BlockResponse{
				ProposedBlock:  pb,
				AttesterCommit: nil,
				PeerFilter:     outgoing,
			}),
			ChannelID: types.ProposeChannel,
		}
		if peer.TrySend(env) {
			e.metrics.ProposalBroadcasts.Add(1)
		} else {
			e.metrics.ProposalBroadcastFailures.Add(1)
			e.logger.Debug("proposal broadcast backpressured", "peer", peer.ID(), "height", pb.Block.Height)
		}
	}
}
