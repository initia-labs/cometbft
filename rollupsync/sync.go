package rollupsync

import (
	"context"
	"fmt"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"

	"github.com/cosmos/gogoproto/proto"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/rollupsync/provider"
	rstypes "github.com/cometbft/cometbft/rollupsync/types"
)

type RollupSyncer struct {
	logger log.Logger
	cfg    config.RollupSyncConfig

	targetBlockHeight uint64

	// immutable
	state sm.State

	blockExec *sm.BlockExecutor
	store     *store.BlockStore
	proxyApp  proxy.AppConns

	batchProvider  rstypes.BatchProvider
	outputProvider rstypes.OutputProvider

	blockCh chan rstypes.BlockInfo
}

func NewRollupSyncer(cfg config.RollupSyncConfig, logger log.Logger, state sm.State, blockExec *sm.BlockExecutor, store *store.BlockStore, proxyApp proxy.AppConns) (*RollupSyncer, error) {
	batchProvider, outputProvider, err := provider.NewProviders(cfg, logger)
	if err != nil {
		return nil, err
	}
	return &RollupSyncer{
		logger: logger,

		state:     state,
		blockExec: blockExec,
		store:     store,
		proxyApp:  proxyApp,

		batchProvider:  batchProvider,
		outputProvider: outputProvider,

		blockCh: make(chan rstypes.BlockInfo, 100),
	}, nil
}

func (rs *RollupSyncer) Start(ctx context.Context) (sm.State, error) {
	targetBlockHeight, err := rs.outputProvider.GetLatestFinalizedBlock(ctx)
	if err != nil {
		return sm.State{}, err
	}
	rs.targetBlockHeight = targetBlockHeight
	if rs.state.LastBlockHeight >= int64(targetBlockHeight) {
		return rs.state, err
	}
	start, _ := rs.blockExec.Store().GetRollupSyncBatchChainHeight()
	start++

	rs.logger.Info("Start rollup sync", "height", targetBlockHeight)

	end, err := rs.batchProvider.GetLastHeight(ctx)
	if err != nil {
		return sm.State{}, err
	}
	rs.logger.Info("L1 Query range", "range", fmt.Sprintf("%d ~ %d", start, end))

	go rs.batchProvider.BatchFetcher(ctx, start, end)
	return rs.sync(ctx)
}

func (rs *RollupSyncer) sync(ctx context.Context) (sm.State, error) {
	batch := make([]byte, 0, MAX_BATCH_BYTES*MAX_BATCH_CHUNK)

	chunks := 0
	state := rs.state

	var lastCommit *types.Commit

	batchCh := rs.batchProvider.GetBatchChannel()
	go func() {
		for batchInfo := range batchCh {
			if batchInfo.Batch == nil {
				rs.blockCh <- rstypes.BlockInfo{
					BatchChainHeight: batchInfo.BatchChainHeight,
				}
				continue
			}
			rs.logger.Debug("Receive a batch chunk")

			chunks++
			batch = append(batch, batchInfo.Batch...)
			rawData, err := DecompressBatch(batch)

			// in the case of failure, a single batch might be separated several chunks of bytes
			// TODO: calculate maximum batch chunks and restrict appendending chunks continuously
			// to avoid a situation that the raw batch is abnormal.
			if err != nil {
				if chunks >= MAX_BATCH_CHUNK {
					rs.logger.Error("error decompressing batch", "error", err)
					return
				}
				continue
			}
			batch = batch[:0]

			dataLength := len(rawData)
			rawBlocks := rawData[:dataLength-1]
			rawCommit := rawData[dataLength-1]

			for _, blockBytes := range rawBlocks {
				pbb := new(cmtproto.Block)
				err := proto.Unmarshal(blockBytes, pbb)
				if err != nil {
					rs.logger.Error("error reading block", "error", err)
					return
				}

				block, err := types.BlockFromProto(pbb)
				if err != nil {
					rs.logger.Error("error from proto block", "error", err)
					return
				}
				rs.blockCh <- rstypes.BlockInfo{
					Block: block,
				}
			}

			pbc := new(cmtproto.Commit)
			err = proto.Unmarshal(rawCommit, pbc)
			if err != nil {
				panic(fmt.Sprintf("error reading block seen commit: %v", err))
			}

			commit, err := types.CommitFromProto(pbc)
			if err != nil {
				panic(fmt.Errorf("converting seen commit: %w", err))
			}
			lastCommit = commit
		}
		rs.logger.Info("Completed fetching batches")
	}()

LOOP:
	for {
		select {
		case <-ctx.Done():
			rs.batchProvider.Quit()
			return state, ctx.Err()
		case blockInfo := <-rs.blockCh:
			block := blockInfo.Block
			if block != nil {
				if state.LastBlockHeight+1 > block.Height {
					continue LOOP
				} else if state.LastBlockHeight+1 < block.Height {
					panic(fmt.Errorf("sync height mismatch; expected %d, got %d", state.LastBlockHeight+1, block.Height))
				}

				blockParts, err := block.MakePartSet(types.BlockPartSizeBytes)
				if err != nil {
					rs.batchProvider.Quit()
					rs.logger.Error("failed to make ",
						"height", block.Height,
						"err", err.Error())
					return state, err
				}
				blockPartSetHeader := blockParts.Header()
				blockID := types.BlockID{Hash: block.Hash(), PartSetHeader: blockPartSetHeader}

				// don't need to save seen commit here, seen commit is used only in consensus.
				rs.store.SaveBlock(block, blockParts, nil)

				state, err = rs.blockExec.ApplyBlock(state, blockID, block)
				if err != nil {
					panic(fmt.Sprintf("Failed to process committed block (%d:%X): %v", block.Height, block.Hash(), err))
				}
				if block.Height == int64(rs.targetBlockHeight) {
					rs.batchProvider.Quit()
					break LOOP
				}
			} else {
				rs.blockExec.Store().SetRollupSyncBatchChainHeight(blockInfo.BatchChainHeight)
			}
		}
	}
	rs.blockExec.Store().SetRollupSyncBatchChainHeight(0)

	rs.store.SaveSeenCommit(state.LastBlockHeight, lastCommit)
	rs.logger.Info("Rollup sync completed!", "height", state.LastBlockHeight)
	return state, nil
}
