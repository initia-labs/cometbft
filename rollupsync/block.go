package rollupsync

import (
	"context"

	"github.com/cometbft/cometbft/types"
)

func (rs *RollupSyncer) blockProcessor(ctx context.Context) error {
	var lastCommit *types.Commit
	var lastBatchInfoIndex, lastBatchChainHeight int64
	defer close(rs.blockChClosed)

LOOP:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case blockInfo := <-rs.blockCh:
			if blockInfo.Block != nil {
				block := blockInfo.Block
				if rs.state.LastBlockHeight+1 != block.Height {
					rs.logger.Debug("block height mismatch", "expected", rs.state.LastBlockHeight+1, "got", block.Height)
					// ignore invalid block
					continue
				}

				lastCommit = block.LastCommit
				if rs.targetBlockHeight != 0 && block.Height == int64(rs.targetBlockHeight)+1 {
					break LOOP
				}

				blockParts, err := block.MakePartSet(types.BlockPartSizeBytes)
				if err != nil {
					rs.logger.Error("failed to make ",
						"height", block.Height,
						"err", err.Error())
					return err
				}
				blockPartSetHeader := blockParts.Header()
				blockID := types.BlockID{Hash: block.Hash(), PartSetHeader: blockPartSetHeader}

				// we don't need to save seen commit here, seen commit is used only in consensus.
				rs.store.SaveBlock(block, blockParts, nil)
				rs.state, err = rs.blockExec.ApplyBlock(rs.state, blockID, block)
				if err != nil {
					return err
				}
			} else if blockInfo.Commit != nil {
				lastCommit = blockInfo.Commit
				if rs.targetBlockHeight != 0 && rs.state.LastBlockHeight == int64(rs.targetBlockHeight) {
					break LOOP
				}
			}

			if lastBatchInfoIndex != blockInfo.BatchInfoIndex || lastBatchChainHeight != blockInfo.BatchChainHeight-1 {
				lastBatchInfoIndex = blockInfo.BatchInfoIndex
				lastBatchChainHeight = blockInfo.BatchChainHeight - 1

				err := rs.blockExec.Store().SetRollupSyncBatchChainHeight(lastBatchInfoIndex, lastBatchChainHeight)
				if err != nil {
					return err
				}
			}
		}
	}

	err := rs.store.SaveSeenCommit(rs.state.LastBlockHeight, lastCommit)
	if err != nil {
		return err
	}

	rs.logger.Info("Rollup sync completed!", "height", rs.state.LastBlockHeight)
	return nil
}
