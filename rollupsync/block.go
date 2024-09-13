package rollupsync

import (
	"context"
	"fmt"

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
				if block.Height <= rs.state.LastBlockHeight {
					if block.Height == 1 {
						rs.logger.Info("ignore genesis block")
						continue
					}

					// end rollup syncer
					return fmt.Errorf("need to rollback to height %d", block.Height-1)
				} else if rs.state.LastBlockHeight+1 < block.Height {
					// rs.logger.Info("block height mismatch", "expected", rs.state.LastBlockHeight+1, "got", block.Height)
					// ignore invalid block
					continue
				}

				lastCommit = block.LastCommit
				if rs.targetBlockHeight != 0 && block.Height == int64(rs.targetBlockHeight)+1 {
					break LOOP
				}

				blockParts, err := block.MakePartSet(types.BlockPartSizeBytes)
				if err != nil {
					rs.logger.Info("failed to make block parts",
						"height", block.Height,
						"err", err.Error())
					continue
				}
				blockPartSetHeader := blockParts.Header()
				blockID := types.BlockID{Hash: block.Hash(), PartSetHeader: blockPartSetHeader}

				rs.state, err = rs.blockExec.ApplyBlock(rs.state, blockID, block)
				if err != nil {
					rs.logger.Error("failed to apply block",
						"height", block.Height,
						"err", err.Error())
					continue
				}
				// we don't need to save seen commit here, seen commit is used only in consensus.
				rs.blockStore.SaveBlock(block, blockParts, nil)
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

	err := rs.blockStore.SaveSeenCommit(rs.state.LastBlockHeight, lastCommit)
	if err != nil {
		return err
	}

	rs.logger.Info("Rollup sync completed!", "height", rs.state.LastBlockHeight)
	return nil
}
