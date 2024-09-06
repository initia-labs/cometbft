package rollupsync

import (
	"context"

	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
)

func (rs *RollupSyncer) blockProcessor(ctx context.Context) (sm.State, error) {
	var lastCommit *types.Commit
	var lastBatchInfoIndex, lastBatchChainHeight int64
LOOP:
	for {
		select {
		// case <-endChecker.C:
		// 	select {
		// 	case <-done:
		// 		if len(rs.blockCh) == 0 {
		// 			return rs.state, errors.New("rollup sync early closed")
		// 		}
		// 	default:
		// 	}

		case <-ctx.Done():
			return rs.state, ctx.Err()
		case blockInfo := <-rs.blockCh:
			if blockInfo.Block != nil {
				block := blockInfo.Block
				if rs.state.LastBlockHeight+1 != block.Height {
					rs.logger.Error("block height mismatch", "expected", rs.state.LastBlockHeight+1, "got", block.Height)
					// ignore invalid block
					continue
				}

				blockParts, err := block.MakePartSet(types.BlockPartSizeBytes)
				if err != nil {
					rs.logger.Error("failed to make ",
						"height", block.Height,
						"err", err.Error())
					return rs.state, err
				}
				blockPartSetHeader := blockParts.Header()
				blockID := types.BlockID{Hash: block.Hash(), PartSetHeader: blockPartSetHeader}

				// we don't need to save seen commit here, seen commit is used only in consensus.
				rs.store.SaveBlock(block, blockParts, nil)
				rs.state, err = rs.blockExec.ApplyBlock(rs.state, blockID, block)
				if err != nil {
					return rs.state, err
				}
			} else if blockInfo.Commit != nil {
				lastCommit = blockInfo.Commit
				if rs.state.LastBlockHeight == int64(rs.targetBlockHeight) {
					break LOOP
				}
			}

			if lastBatchInfoIndex == 0 && lastBatchChainHeight == 0 {
				lastBatchInfoIndex = blockInfo.BatchInfoIndex
				lastBatchChainHeight = blockInfo.BatchChainHeight
			} else if lastBatchInfoIndex != blockInfo.BatchInfoIndex || lastBatchChainHeight != blockInfo.BatchChainHeight {
				err := rs.blockExec.Store().SetRollupSyncBatchChainHeight(lastBatchInfoIndex, lastBatchChainHeight)
				if err != nil {
					return rs.state, err
				}
			}
		}
	}

	err := rs.store.SaveSeenCommit(rs.state.LastBlockHeight, lastCommit)
	if err != nil {
		return rs.state, err
	}

	rs.logger.Info("Rollup sync completed!", "height", rs.state.LastBlockHeight)
	return rs.state, nil
}
