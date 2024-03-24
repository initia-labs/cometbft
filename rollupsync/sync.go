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
	"github.com/cometbft/cometbft/rollupsync/provider"
	rstypes "github.com/cometbft/cometbft/rollupsync/types"
)

type RollupSyncer struct {
	logger log.Logger
	cfg    config.RollupSyncConfig

	targetBlockHeight uint64

	// immutable
	initialState sm.State

	blockExec *sm.BlockExecutor
	store     *store.BlockStore

	batchProvider  rstypes.BatchProvider
	outputProvider rstypes.OutputProvider

	blockCh chan *types.Block
}

func NewRollupSyncer(cfg config.RollupSyncConfig, logger log.Logger, state sm.State, blockExec *sm.BlockExecutor, store *store.BlockStore) (*RollupSyncer, error) {
	batchProvider, outputProvider, err := provider.NewProviders(cfg, logger)
	if err != nil {
		return nil, err
	}
	return &RollupSyncer{
		logger: logger,

		initialState: state,
		blockExec:    blockExec,
		store:        store,

		batchProvider:  batchProvider,
		outputProvider: outputProvider,

		blockCh: make(chan *types.Block, 100),
	}, nil
}

func (rs *RollupSyncer) Start(ctx context.Context) (sm.State, error) {
	targetBlockHeight, err := rs.outputProvider.GetLatestFinalizedBlock(ctx)
	if err != nil {
		return sm.State{}, err
	}
	rs.targetBlockHeight = targetBlockHeight

	rs.logger.Info("Start rollup sync", "height", targetBlockHeight)
	go rs.batchProvider.BatchFetcher(ctx)

	return rs.sync(ctx)
}

func (rs *RollupSyncer) sync(ctx context.Context) (sm.State, error) {
	batch := make([]byte, 0, MAX_BATCH_BYTES*MAX_BATCH_CHUNK)

	chunks := 0
	state := rs.initialState
	var block *types.Block

	batchCh := rs.batchProvider.GetBatchChannel()
	go func() {
		for batchChunk := range batchCh {
			if batchChunk == nil || len(batchChunk) == 0 {
				continue
			}
			rs.logger.Debug("Receive a batch chunk")

			chunks++
			batch = append(batch, batchChunk...)
			rawBlocks, err := DecompressBatch(batch)

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
				rs.blockCh <- block
			}
		}
		rs.logger.Info("Completed fetching batches")
	}()

LOOP:
	for {
		select {
		case <-ctx.Done():
			return state, nil
		case nextBlock := <-rs.blockCh:
			if block != nil {
				if state.LastBlockHeight > 0 && state.LastBlockHeight+1 != block.Height {
					panic(fmt.Errorf("sync height mismatch; expected %d, got %d", state.LastBlockHeight+1, block.Height))
				}

				blockParts, err := block.MakePartSet(types.BlockPartSizeBytes)
				if err != nil {
					rs.logger.Error("failed to make ",
						"height", block.Height,
						"err", err.Error())
					return state, err
				}
				blockPartSetHeader := blockParts.Header()
				blockID := types.BlockID{Hash: block.Hash(), PartSetHeader: blockPartSetHeader}

				rs.store.SaveBlock(block, blockParts, nextBlock.LastCommit)

				state, err = rs.blockExec.ApplyBlock(state, blockID, block)
				if err != nil {
					panic(fmt.Sprintf("Failed to process committed block (%d:%X): %v", block.Height, block.Hash(), err))
				}
			}
			block = nextBlock

			if block.Height == int64(rs.targetBlockHeight) {
				break LOOP
			}
		}
	}
	rs.logger.Info("Rollup sync completed!", "height", state.LastBlockHeight)

	return state, nil
}
