package engine

import (
	"time"

	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/sequencing/types"
	cmttypes "github.com/cometbft/cometbft/types"
)

func (b *Engine) handleStatusUpdate(peer p2p.Peer, su *types.StatusUpdate) {
	if peer == nil || su == nil {
		return
	}

	b.peerSet.Update(peer.ID(), su.BaseHeight, su.LastHeight)

	if ps, ok := peer.Get(cmttypes.PeerStateKey).(*types.PeerHeight); ok {
		ps.SetHeight(su.LastHeight)
	}
}

func (p *Engine) handleBlockRequest(peer p2p.Peer, req *types.BlockRequest) {
	if peer == nil || req == nil {
		return
	}

	block := p.blockStore.LoadBlock(req.Height)
	if block == nil {
		return
	}
	// BlockStore persists block h plus last commit (h-1) and seen commit h, so we
	// must load the seen commit at the requested height to return the block's
	// current commit.
	commit := p.blockStore.LoadSeenCommit(req.Height)
	if commit == nil {
		return
	}

	// The extended commit already encloses attestor signatures, so there is no
	// separate attestor payload to attach here. Skip adding the requester to the
	// provenance list so downstream nodes treat this as a direct reply rather
	// than fresh gossip.
	res := &types.BlockResponse{
		ProposedBlock: &types.ProposedBlock{
			Block:  block,
			Commit: commit.WrappedExtendedCommit(),
		},
	}

	peer.Send(p2p.Envelope{
		Message:   types.MsgToProto(res),
		ChannelID: types.SyncChannel,
	})
}

func (p *Engine) handleBlockResponse(peer p2p.Peer, res *types.BlockResponse) {
	if peer == nil || res == nil {
		return
	}

	storeHeight := p.blockStore.Height()
	if res.ProposedBlock != nil && res.ProposedBlock.Block != nil && res.ProposedBlock.Commit != nil {
		height := res.ProposedBlock.Block.Height
		p.peerSet.RecordResponse(peer.ID(), height, time.Now())

		// only accept blocks that are not too far in the future
		if height > storeHeight && height <= storeHeight+maxFutureBlocks*2 {
			p.blockBucket.Add(peer.ID(), height, res.ProposedBlock)
		}

		p.requestWindow.Release(height)
	}

	if res.AttesterCommit != nil && res.AttesterCommit.Commit != nil {
		height := res.AttesterCommit.Commit.Height

		// only accept attestor commits for blocks that are not too far in the future
		if height <= storeHeight+maxFutureBlocks*2 {
			p.commitBucket.Add(peer.ID(), height, res.AttesterCommit)
		}
	}
}
