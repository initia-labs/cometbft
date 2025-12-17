package engine

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/cometbft/cometbft/p2p"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/sequencing/types"
	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
)

var upgradeNeededRegex = regexp.MustCompile(`UPGRADE .* NEEDED`)

// applyProposedBlock applies a proposed block message from a peer.
func (e *Engine) applyProposedBlock(pb *types.ProposedBlock) (badPeer, applied, upgrade bool) {
	if pb == nil || pb.Block == nil || pb.Commit == nil {
		return false, false, false
	}

	e.stateMu.Lock()
	state := e.state.Copy()
	e.stateMu.Unlock()

	if pb.Block.Height <= state.LastBlockHeight {
		return false, false, false
	}

	// validate the block
	blockParts, err := pb.Block.MakePartSet(cmttypes.BlockPartSizeBytes)
	if err != nil {
		e.logger.Error("failed to make block parts", "height", pb.Block.Height, "err", err)

		return true, false, false
	}

	blockID := cmttypes.BlockID{Hash: pb.Block.Hash(), PartSetHeader: blockParts.Header()}
	if err := state.Validators.VerifySequencerCommit(state.ChainID, blockID, pb.Block.Height, pb.Commit.ToCommit()); err != nil {
		e.logger.Error("failed to validate commit", "height", pb.Block.Height, "err", err)

		return true, false, false
	}
	if err := e.blockExec.ValidateBlock(state, pb.Block); err != nil {
		panic(fmt.Sprintf("CONSENSUS FAILURE!!! Proposed block failed validation: block (%d:%X): %v", pb.Block.Height, pb.Block.Hash(), err))
	}

	// store the block with the validator set
	e.blockStore.SaveBlockWithValidatorSet(pb.Block, blockParts, pb.Commit.ToCommit(), state.Validators)

	// apply the block
	state, err = e.blockExec.ApplyVerifiedBlock(state, blockID, pb.Block)
	if err != nil {
		// when an upgrade is needed, we do not panic, just log and stop the engine
		if upgradeNeededRegex.MatchString(err.Error()) {
			e.logger.Error("node upgrade required", "height", pb.Block.Height, "err", err)
			_ = e.Stop()
			return false, false, true
		}

		panic(fmt.Sprintf("Failed to process committed block (%d:%X): %v", pb.Block.Height, pb.Block.Hash(), err))
	}

	e.stateMu.Lock()
	*e.state = state
	e.lastProposedBlockHeight = pb.Block.Height
	e.lastProposedBlockTime = pb.Block.Time
	e.lastProposedBlockNumTxs = len(pb.Block.Data.Txs)

	// try to update our role in case validator set changed
	e.switchRoleLocked()
	e.stateMu.Unlock()

	e.metrics.recordBlockMetrics(pb.Block)

	// signal the block has been applied
	e.signalBlockApplied()

	return false, true, false
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
	blockID, err := blockID(block)
	if err != nil {
		e.logger.Error("unable to compute block ID", "height", ac.Commit.Height, "err", err)
		return false, false
	}
	if !blockID.Equals(ac.Commit.BlockID) {
		// block hash mismatch
		return true, false
	}
	vals := e.blockStore.LoadValidatorSet(block.ValidatorsHash)
	if vals == nil {
		e.logger.Error("failed to load validator set for attestor commit", "height", ac.Commit.Height)
		return false, false
	}

	var updated *cmttypes.ExtendedCommit
	incoming := ac.Commit
	commit := e.blockStore.LoadSeenCommit(ac.Commit.Height)
	if commit != nil {
		updated = commit.WrappedExtendedCommit()
	} else {
		updated = ac.Commit.Clone()
		updated.ExtendedSignatures = make([]cmttypes.ExtendedCommitSig, len(vals.Validators))
		for i := range updated.ExtendedSignatures {
			updated.ExtendedSignatures[i] = cmttypes.NewExtendedCommitSigAbsent()
		}
	}

	// check and merge signatures
	if len(updated.ExtendedSignatures) != len(incoming.ExtendedSignatures) {
		return true, false
	}

	for idx, sig := range incoming.ExtendedSignatures {
		current := updated.ExtendedSignatures[idx]

		// skip it if we already have a commit signature
		if current.BlockIDFlag != cmttypes.BlockIDFlagAbsent {
			continue
		}

		// skip absent signatures
		if sig.BlockIDFlag == cmttypes.BlockIDFlagAbsent {
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
	if idx == -1 || attestor.VotingPower != cmttypes.AttestorVotingPower {
		return
	}

	commit := e.blockStore.LoadSeenCommit(height)
	if commit != nil {
		for _, sig := range commit.Signatures {
			if bytes.Equal(sig.ValidatorAddress, attesterAddr) && sig.BlockIDFlag != cmttypes.BlockIDFlagAbsent {
				// already signed
				return
			}
		}
	}

	// compute block ID
	blockID, err := blockID(block)
	if err != nil {
		e.logger.Error("unable to compute block ID", "height", height, "err", err)
		return
	}

	vote := &cmttypes.Vote{
		ValidatorAddress: attesterAddr,
		ValidatorIndex:   idx,
		Height:           height,
		Round:            0,
		Timestamp:        voteTime(block.Time),
		Type:             cmtproto.PrecommitType,
		BlockID:          blockID,
	}

	v := vote.ToProto()
	if err = e.privValidator.SignVote(chainID, v); err != nil {
		e.logger.Error("unable to sign attestor vote", "height", height, "err", err)
		return
	}

	vote.Timestamp = v.Timestamp
	vote.Signature = v.Signature
	vote.ExtensionSignature = v.ExtensionSignature
	if commit == nil {
		sigs := make([]cmttypes.CommitSig, validators.Size())
		for i := range sigs {
			sigs[i] = cmttypes.NewCommitSigAbsent()
		}
		commit = &cmttypes.Commit{
			Height:     height,
			BlockID:    blockID,
			Signatures: sigs,
		}
	} else if len(commit.Signatures) != validators.Size() {
		sigs := make([]cmttypes.CommitSig, validators.Size())
		for i := range sigs {
			sigs[i] = cmttypes.NewCommitSigAbsent()
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
	attestorCommit := &types.AttestorCommit{
		Commit: commit.WrappedExtendedCommit(),
	}
	e.broadcastAttestorCommit(attestorCommit)
}

// produce next block if we are a sequencer
func (e *Engine) proposeBlock() {
	if !e.isSequencer.Load() {
		return
	}

	e.stateMu.Lock()
	// check one more time after holding the lock
	if !e.isSequencer.Load() {
		e.stateMu.Unlock()
		return
	}
	height := e.state.LastBlockHeight + 1
	if height <= e.lastProposedBlockHeight {
		e.stateMu.Unlock()
		return
	}
	timePassed := cmttime.Now().Sub(e.lastProposedBlockTime)
	if timePassed < e.cfg.BlockInterval {
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
	if idx == -1 || validator.VotingPower != cmttypes.SequencerVotingPower {
		return
	}

	if height == 1 {
		height = state.InitialHeight
	}

	var lastExtCommit *cmttypes.ExtendedCommit
	if height == state.InitialHeight {
		// We're creating a proposal for the first block.
		// The commit is empty, but not nil.
		lastExtCommit = &cmttypes.ExtendedCommit{}
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
	proposedBlockID, err := blockID(proposedBlock)
	if err != nil {
		e.logger.Error("unable to compute block ID", "height", height, "err", err)
		return
	}

	// create self vote
	vote := &cmttypes.Vote{
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
		if !ignoreSignErr(err) {
			e.logger.Error("unable to sign proposal vote", "height", height, "err", err)
		} else {
			e.logger.Debug("ignoring error signing proposal vote", "height", height, "err", err)
		}
		return
	}

	vote.Timestamp = v.Timestamp
	vote.Signature = v.Signature
	vote.ExtensionSignature = v.ExtensionSignature
	signatures := make([]cmttypes.CommitSig, state.Validators.Size())
	for i := range signatures {
		signatures[i] = cmttypes.NewCommitSigAbsent()
	}
	commit := &cmttypes.Commit{
		Height:     height,
		BlockID:    proposedBlockID,
		Signatures: signatures,
	}
	commit.Signatures[idx] = vote.CommitSig()

	proposed := &types.ProposedBlock{
		Block:  proposedBlock,
		Commit: commit.WrappedExtendedCommit(),
	}

	// add block to bucket as well
	e.blockBucket.Add(types.SELF_PEER_ID, height, proposed)

	// broadcast the proposed block
	e.broadcastProposedBlock(proposed)

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

// blockID computes the BlockID for a given block.
func blockID(block *cmttypes.Block) (cmttypes.BlockID, error) {
	if block == nil {
		return cmttypes.BlockID{}, fmt.Errorf("nil block")
	}
	parts, err := block.MakePartSet(cmttypes.BlockPartSizeBytes)
	if err != nil {
		return cmttypes.BlockID{}, err
	}
	return cmttypes.BlockID{Hash: block.Hash(), PartSetHeader: parts.Header()}, nil
}

// ignoreSignErr returns true if the error is safe to ignore.
func ignoreSignErr(err error) bool {
	if err == nil {
		return false
	}
	// already signed by other instance
	ignoreErrors := []string{
		"regression",
		"double signing",   // tmkms
		"conflicting data", // cometkms
	}
	for _, substr := range ignoreErrors {
		if strings.Contains(err.Error(), substr) {
			return true
		}
	}
	return false
}

// checkConflictingVotes checks whether the given commit has any conflicting votes
// compared to the already stored commit for the same height. If so, it reports
// them to the reactor.
func (e *Engine) checkConflictingVotes(pid p2p.ID, commit *cmttypes.Commit) {
	if commit == nil {
		return
	}
	// load the commit for this height
	currentCommit := e.blockStore.LoadSeenCommit(commit.Height)
	if currentCommit == nil {
		return
	}
	// check if the block IDs differ
	if currentCommit.BlockID.Equals(commit.BlockID) {
		return
	}

	// if it is different, check each votes
	block := e.blockStore.LoadBlock(commit.Height)
	if block == nil {
		return
	}
	vals := e.blockStore.LoadValidatorSet(block.ValidatorsHash)
	if vals == nil {
		return
	}

	found := false
	for i, sig := range commit.Signatures {
		vote := commit.GetVote(int32(i))
		if vote == nil {
			continue
		}

		valIdx, val := vals.GetByAddress(sig.ValidatorAddress)
		if valIdx == -1 || val == nil || val.PubKey == nil {
			continue
		}
		currentVote := currentCommit.GetVote(valIdx)
		if currentVote == nil {
			continue
		}

		// if one of them is not a commit, it is not conflicting
		if currentVote.CommitSig().BlockIDFlag != cmttypes.BlockIDFlagCommit || vote.CommitSig().BlockIDFlag != cmttypes.BlockIDFlagCommit {
			continue
		}

		// if it is conflicting, then verify the signature
		if err := vote.Verify(e.chainID, val.PubKey); err != nil {
			e.logger.Debug("conflicting vote failed verification", "height", commit.Height, "val_index", valIdx, "err", err)
			continue
		}

		// report conflicting vote
		e.logger.Info("detected conflicting vote", "height", commit.Height, "val_index", valIdx, "val_addr", sig.ValidatorAddress, "current_block_id", currentCommit.BlockID, "commit_block_id", commit.BlockID)
		e.reactor.ReportConflictingVotes(currentVote, vote)
		found = true
	}

	if found {
		e.flagBadPeer(pid, "sent commit with conflicting votes")
	}
}
