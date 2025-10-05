package engine

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/sequencing/types"
	cmtstate "github.com/cometbft/cometbft/state"
	comettypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
)

// applyProposedBlock applies a proposed block message from a peer.
func (e *Engine) applyProposedBlock(pb *types.ProposedBlock) (badPeer bool, applied bool) {
	if pb == nil || pb.Block == nil || pb.Commit == nil {
		return false, false
	}

	e.stateMu.Lock()
	state := e.state.Copy()
	e.stateMu.Unlock()

	if pb.Block.Height <= state.LastBlockHeight {
		return false, false
	}

	// validate the block
	blockParts, err := pb.Block.MakePartSet(comettypes.BlockPartSizeBytes)
	if err != nil {
		e.logger.Error("failed to make block parts", "height", pb.Block.Height, "err", err)

		return true, false
	}

	blockID := comettypes.BlockID{Hash: pb.Block.Hash(), PartSetHeader: blockParts.Header()}
	if err := state.Validators.VerifySequencerCommit(state.ChainID, blockID, pb.Block.Height, pb.Commit.ToCommit()); err != nil {
		e.logger.Error("failed to validate commit", "height", pb.Block.Height, "err", err)

		return true, false
	}
	if err := e.blockExec.ValidateBlock(state, pb.Block); err != nil {
		e.logger.Error("failed to validate proposed block", "height", pb.Block.Height, "err", err, "block", pb.Block.String())

		return true, false
	}

	// store the block with the validator set
	e.blockStore.SaveBlockWithValidatorSet(pb.Block, blockParts, pb.Commit.ToCommit(), state.NextValidators)

	// apply the block
	state, err = e.blockExec.ApplyVerifiedBlock(state, blockID, pb.Block)
	if err != nil {
		panic(fmt.Sprintf("Failed to process committed block (%d:%X): %v", pb.Block.Height, pb.Block.Hash(), err))
	}

	e.stateMu.Lock()
	*e.state = state
	e.lastProposedBlockHeight = pb.Block.Height
	e.lastProposedBlockTime = cmtstate.MedianTime(pb.Commit.ToCommit(), state.LastValidators)
	e.lastProposedBlockNumTxs = len(pb.Block.Data.Txs)
	e.stateMu.Unlock()

	// try to update our role in case validator set changed
	e.switchRole()

	e.metrics.recordBlockMetrics(pb.Block)

	// signal the block has been applied
	e.signalBlockApplied()

	return false, true
}

// applyAttestorCommit applies an attestor commit message from a peer.
// NOTE: use blockstore, so don't need to depend on state.
func (e *Engine) applyAttestorCommit(ac *types.AttestorCommit) (badPeer bool, applied bool) {
	if ac == nil || ac.Commit == nil {
		return false, false
	}

	block := e.blockStore.LoadBlock(ac.Commit.Height)
	if block == nil {
		e.logger.Error("no block found for attestor commit", "height", ac.Commit.Height)
		return false, false
	}
	vals := e.blockStore.LoadValidatorSet(block.ValidatorsHash)
	if vals == nil {
		e.logger.Error("failed to load validator set for attestor commit", "height", ac.Commit.Height)
		return false, false
	}

	var updated *comettypes.ExtendedCommit
	incoming := ac.Commit
	commit := e.blockStore.LoadSeenCommit(ac.Commit.Height)
	if commit != nil {
		updated = commit.WrappedExtendedCommit()
	} else {
		updated = ac.Commit.Clone()
		updated.ExtendedSignatures = make([]comettypes.ExtendedCommitSig, len(vals.Validators))
		for i := range updated.ExtendedSignatures {
			updated.ExtendedSignatures[i] = comettypes.NewExtendedCommitSigAbsent()
		}
	}

	for idx, sig := range incoming.ExtendedSignatures {
		current := updated.ExtendedSignatures[idx]

		// skip it if we already have a commit signature
		if current.BlockIDFlag != comettypes.BlockIDFlagAbsent {
			continue
		}

		// skip absent signatures
		if sig.BlockIDFlag == comettypes.BlockIDFlagAbsent {
			continue
		}

		if err := sig.ValidateBasic(); err != nil {
			return true, false
		}
		expectedAddr, val := vals.GetByIndex(int32(idx))
		if expectedAddr == nil || val == nil {
			return true, false
		}
		if !bytes.Equal(expectedAddr, sig.ValidatorAddress) {
			return true, false
		}
		vote := incoming.GetExtendedVote(int32(idx))
		if vote == nil {
			return true, false
		}
		if err := vote.Verify(e.chainID, val.PubKey); err != nil {
			return true, false
		}

		// all good, merge it in
		updated.ExtendedSignatures[idx] = sig
		applied = true
	}

	// save the updated commit if we changed it
	if applied {
		if err := e.blockStore.SaveSeenCommit(ac.Commit.Height, updated.ToCommit()); err != nil {
			e.logger.Error("failed to save attestor commit", "height", ac.Commit.Height, "err", err)
		}
	}

	return false, applied
}

// keep checking latest seen commit and see whether we did sign it,
// if not, and we are an attestor, sign and broadcast it.
func (e *Engine) attestBlock() {
	if !e.isAttestor.Load() {
		return
	}

	chainID := e.chainID
	height := e.blockStore.Height()
	block := e.blockStore.LoadBlock(height)
	if block == nil {
		return
	}

	validators := e.blockStore.LoadValidatorSet(block.ValidatorsHash)
	if validators == nil {
		e.logger.Error("failed to load validator set for attestation", "height", height)
		return
	}

	attesterAddr := e.privValidatorPubKey.Address()
	idx, attestor := validators.GetByAddress(attesterAddr)
	if idx == -1 || attestor.VotingPower != comettypes.AttestorVotingPower {
		return
	}

	commit := e.blockStore.LoadSeenCommit(height)
	if commit != nil {
		for _, sig := range commit.Signatures {
			if bytes.Equal(sig.ValidatorAddress, attesterAddr) && sig.BlockIDFlag != comettypes.BlockIDFlagAbsent {
				// already signed
				return
			}
		}
	}

	blockParts, err := block.MakePartSet(comettypes.BlockPartSizeBytes)
	if err != nil {
		e.logger.Error("unable to create proposal block part set", "error", err)
		return
	}

	propBlockID := comettypes.BlockID{Hash: block.Hash(), PartSetHeader: blockParts.Header()}
	vote := &comettypes.Vote{
		ValidatorAddress: attesterAddr,
		ValidatorIndex:   idx,
		Height:           height,
		Round:            0,
		Timestamp:        voteTime(block.Time),
		Type:             cmtproto.PrecommitType,
		BlockID:          propBlockID,
	}

	v := vote.ToProto()
	if err = e.privValidator.SignVote(chainID, v); err != nil {
		if strings.Contains(err.Error(), "exhausted all attempts") {
			panic(fmt.Sprintf("Failed to sign attestor vote: %v", err))
		}

		e.logger.Error("unable to sign attestor vote", "height", height, "err", err)
		return
	}

	vote.Signature = v.Signature
	if commit == nil {
		sigs := make([]comettypes.CommitSig, validators.Size())
		for i := range sigs {
			sigs[i] = comettypes.NewCommitSigAbsent()
		}
		commit = &comettypes.Commit{
			Height:     height,
			BlockID:    propBlockID,
			Signatures: sigs,
		}
	} else if len(commit.Signatures) != validators.Size() {
		sigs := make([]comettypes.CommitSig, validators.Size())
		for i := range sigs {
			sigs[i] = comettypes.NewCommitSigAbsent()
		}
		copy(sigs, commit.Signatures)
		commit.Signatures = sigs
	}
	commit.Signatures[idx] = vote.CommitSig()

	// save the commit
	if err = e.blockStore.SaveSeenCommit(height, commit); err != nil {
		e.logger.Error("failed to save attestor commit", "height", height, "err", err)
		return
	}

	// broadcast it
	e.broadcastAttestorCommit(&types.AttestorCommit{
		Commit: commit.WrappedExtendedCommit(),
	})
}

// produce next block if we are a sequencer
func (e *Engine) proposeBlock() {
	if !e.isSequencer.Load() {
		return
	}

	e.stateMu.Lock()

	height := e.state.LastBlockHeight + 1
	if height <= e.lastProposedBlockHeight {
		e.stateMu.Unlock()
		return
	}
	timePassed := cmttime.Now().Sub(e.lastProposedBlockTime)
	if timePassed < e.cfg.TimeoutPropose {
		e.stateMu.Unlock()
		return
	}

	// don't propose empty blocks too often
	if !e.cfg.CreateEmptyBlocks &&
		e.lastProposedBlockNumTxs == 0 &&
		e.reactor.MempoolSize() == 0 &&
		timePassed < e.cfg.CreateEmptyBlocksInterval {
		e.stateMu.Unlock()
		return
	}

	state := e.state.Copy()
	e.stateMu.Unlock()

	proposerAddr := e.privValidatorPubKey.Address()
	idx, validator := state.Validators.GetByAddress(proposerAddr)
	if idx == -1 || validator.VotingPower != comettypes.SequencerVotingPower {
		return
	}

	if height == 1 {
		height = state.InitialHeight
	}

	var lastExtCommit *comettypes.ExtendedCommit
	if height == state.InitialHeight {
		// We're creating a proposal for the first block.
		// The commit is empty, but not nil.
		lastExtCommit = &comettypes.ExtendedCommit{}
	} else {
		// Make the commit from LastCommit.
		lastCommit := e.blockStore.LoadSeenCommit(height - 1)
		if lastCommit == nil {
			panic(fmt.Sprintf("Failed to load last commit for proposal: height %d", height-1))
		}

		lastExtCommit = lastCommit.WrappedExtendedCommit()
	}

	e.logger.Info("proposing block", "height", height, "proposer", validator.Address)
	proposedBlock, err := e.blockExec.CreateProposalBlock(
		context.Background(),
		height,
		state,
		lastExtCommit,
		proposerAddr,
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to create proposal block: height %d err %v", height, err))
	}

	blockParts, err := proposedBlock.MakePartSet(comettypes.BlockPartSizeBytes)
	if err != nil {
		e.logger.Error("unable to create proposal block part set", "error", err)
		return
	}

	proposedBlockID := comettypes.BlockID{Hash: proposedBlock.Hash(), PartSetHeader: blockParts.Header()}
	vote := &comettypes.Vote{
		ValidatorAddress: proposerAddr,
		ValidatorIndex:   idx,
		Height:           height,
		Round:            0,
		Timestamp:        voteTime(proposedBlock.Time),
		Type:             cmtproto.PrecommitType,
		BlockID:          proposedBlockID,
	}

	v := vote.ToProto()
	if err = e.privValidator.SignVote(state.ChainID, v); err != nil {
		if strings.Contains(err.Error(), "exhausted all attempts") {
			panic(fmt.Sprintf("Failed to sign proposal vote: %v", err))
		}
		if !strings.Contains(err.Error(), "regression") {
			e.logger.Error("unable to sign proposal vote", "height", height, "err", err)
		}
		return
	}

	vote.Signature = v.Signature
	signatures := make([]comettypes.CommitSig, state.Validators.Size())
	for i := range signatures {
		signatures[i] = comettypes.NewCommitSigAbsent()
	}
	commit := &comettypes.Commit{
		Height:     height,
		BlockID:    proposedBlockID,
		Signatures: signatures,
	}
	commit.Signatures[idx] = vote.CommitSig()

	// add block to bucket as well
	e.blockBucket.Add(types.SELF_PEER_ID, height, &types.ProposedBlock{
		Block:  proposedBlock,
		Commit: commit.WrappedExtendedCommit(),
	})

	// broadcast the proposed block
	e.broadcastProposedBlock(&types.ProposedBlock{
		Block:  proposedBlock,
		Commit: commit.WrappedExtendedCommit(),
	})

	// keep track of last proposed height to prevent entering this function too often
	e.stateMu.Lock()
	e.lastProposedBlockHeight = height
	e.stateMu.Unlock()
}

func voteTime(lastBlockTime time.Time) time.Time {
	now := cmttime.Now()

	const timeIota = time.Millisecond
	minVoteTime := lastBlockTime.Add(timeIota)

	if now.After(minVoteTime) {
		return now
	}

	return minVoteTime
}
