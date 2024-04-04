package rollupsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"

	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/rollupsync/provider"
	rstypes "github.com/cometbft/cometbft/rollupsync/types"
)

type RollupSyncer struct {
	logger log.Logger
	cfg    *config.RollupSyncConfig

	targetBlockHeight uint64

	// immutable
	state sm.State

	blockExec *sm.BlockExecutor
	store     *store.BlockStore
	proxyApp  proxy.AppConns

	l1Provider *provider.L1Provider

	batchCh chan rstypes.BatchChanInfo
	blockCh chan rstypes.BlockChanInfo
}

func NewRollupSyncer(cfg *config.RollupSyncConfig, logger log.Logger, state sm.State, blockExec *sm.BlockExecutor, store *store.BlockStore, proxyApp proxy.AppConns) (*RollupSyncer, error) {
	l1Provider, err := provider.NewL1Provider(logger, cfg)
	if err != nil {
		return nil, err
	}

	return &RollupSyncer{
		logger: logger,
		cfg:    cfg,

		state:     state,
		blockExec: blockExec,
		store:     store,
		proxyApp:  proxyApp,

		l1Provider: l1Provider,

		batchCh: make(chan rstypes.BatchChanInfo, 100),
		blockCh: make(chan rstypes.BlockChanInfo, 1000),
	}, nil
}

func (rs *RollupSyncer) Start(ctx context.Context) (sm.State, error) {
	// fetch last finalized block height
	targetBlockHeight, err := rs.l1Provider.GetLatestFinalizedBlock(ctx)
	if err != nil {
		return sm.State{}, err
	}

	rs.logger.Info("start rollup sync", "initialHeight", rs.state.LastBlockHeight+1, "target", targetBlockHeight)
	rs.targetBlockHeight = targetBlockHeight

	// if the target block height is already reached, return the current state
	if rs.state.LastBlockHeight >= int64(targetBlockHeight) {
		return rs.state, err
	}

	// load batch info updates from l1
	batchInfoUpdates, err := rs.l1Provider.GetBatchInfoUpdates(ctx, int64(targetBlockHeight))
	if err != nil {
		return sm.State{}, err
	}

	rs.logger.Info("batch info updates", "history", batchInfoUpdates.String())
	return rs.blockSync(ctx, batchInfoUpdates)
}

func (rs RollupSyncer) batchProvider(chain string) (rstypes.BatchProvider, error) {
	switch chain {
	case rstypes.CHAIN_NAME_L1:
		return rs.l1Provider, nil
	case rstypes.CHAIN_NAME_CELESTIA:
		return provider.NewCelestiaProvider(rs.logger.With("provider", rstypes.CHAIN_NAME_CELESTIA), rs.cfg)
	}

	return nil, errors.New("not implemented")
}

func (rs *RollupSyncer) fetchBatches(ctx context.Context, batchInfoUpdates rstypes.BatchInfoUpdates) error {
	height := rs.state.LastBlockHeight + 1
	batchChainStartHeight, _ := rs.blockExec.Store().GetRollupSyncBatchChainHeight()
	batchChainStartHeight++

	for _, batchInfoUpdate := range batchInfoUpdates {
		if batchInfoUpdate.End < height {
			continue
		}

		batchProvider, err := rs.batchProvider(batchInfoUpdate.Chain)
		if err != nil {
			return err
		}
		batchProvider.SetSubmitter(batchInfoUpdate.Submitter)
		batchChainLastHeight, err := batchProvider.GetLastHeight(ctx)
		if err != nil {
			return err
		}

		rs.logger.Info("batch info", "height", batchInfoUpdate.Start, "chain", batchInfoUpdate.Chain, "submitter", batchInfoUpdate.Submitter)
		rs.logger.Info("batch chain query range", "chain", batchInfoUpdate.Chain, "range", fmt.Sprintf("%d ~ %d", batchChainStartHeight, batchChainLastHeight))

		err = rs.fetchBatch(ctx, batchInfoUpdate.End, batchProvider, batchChainStartHeight, batchChainLastHeight)
		if err != nil {
			return err
		}

		height = batchInfoUpdate.End + 1
		batchChainStartHeight = 1
	}

	return nil
}

func (rs *RollupSyncer) fetchBatch(ctx context.Context, targetL2Height int64, batchProvider rstypes.BatchProvider, batchChainStartHeight int64, batchChainEndHeight int64) error {
	bpCtx, cancelProvider := context.WithCancel(ctx)
	defer cancelProvider()

	done := make(chan struct{})
	go func() {
		err := batchProvider.BatchFetcher(bpCtx, rs.batchCh, batchChainStartHeight, batchChainEndHeight)
		if err != nil {
			rs.logger.Error("batch provider", "fetcher", err.Error())
		}

		close(done)
	}()

	chunks := 0
	batch := make([]byte, 0, rs.cfg.MaxBatchChunkBytes*rs.cfg.MaxBatchChunkNum)
	endChecker := time.NewTicker(100 * time.Millisecond)
	defer endChecker.Stop()

BATCH_LOOP:
	for {
		select {
		case <-endChecker.C:
			select {
			case <-done:
				if len(rs.batchCh) == 0 {
					return errors.New("batch provider early closed")
				}
			default:
			}

		case <-ctx.Done():
			return ctx.Err()
		case batchInfo := <-rs.batchCh:
			if batchInfo.Batch == nil {
				rs.blockCh <- rstypes.BlockChanInfo{
					BatchChainHeight: batchInfo.BatchChainHeight,
				}

				continue
			}

			rs.logger.Debug("received a batch chunk")

			chunks++
			batch = append(batch, batchInfo.Batch...)
			rawData, err := decompressBatch(batch)
			if err != nil {
				if chunks >= int(rs.cfg.MaxBatchChunkNum) {
					return err
				}

				continue
			}

			// cleanup batch chunks
			batch = batch[:0]
			chunks = 0

			dataLength := len(rawData)
			rawBlocks := rawData[:dataLength-1]
			rawCommit := rawData[dataLength-1]

			for i, blockBytes := range rawBlocks {
				block, err := unmarshalBlock(blockBytes)
				if err != nil {
					return err
				}

				rs.blockCh <- rstypes.BlockChanInfo{
					Block: block,
				}

				// if the block is reached to target height, break the loop
				// and send the last commit.
				if block.Height == targetL2Height {
					commit := new(types.Commit)
					if i == len(rawBlocks)-1 {
						commit, err = unmarshalCommit(rawCommit)
						if err != nil {
							return err
						}
					} else {
						// extract last commit from the next block
						nextBlock, err := unmarshalBlock(rawBlocks[i+1])
						if err != nil {
							return err
						}

						commit = nextBlock.LastCommit
					}

					rs.blockCh <- rstypes.BlockChanInfo{
						Commit: commit,
					}

					break BATCH_LOOP
				}
			}
		}
	}

	rs.logger.Info("Completed fetching batches")
	return nil
}

func (rs *RollupSyncer) blockSync(ctx context.Context, batchInfoUpdates rstypes.BatchInfoUpdates) (sm.State, error) {
	batchCtx, cancelBatchSync := context.WithCancel(ctx)
	defer cancelBatchSync()

	done := make(chan struct{})
	go func() {
		err := rs.fetchBatches(batchCtx, batchInfoUpdates)
		if err != nil {
			rs.logger.Error("batch sync", "error", err.Error())
		}

		close(done)
	}()

	endChecker := time.NewTicker(100 * time.Millisecond)

	var lastCommit *types.Commit
LOOP:
	for {
		select {
		case <-endChecker.C:
			select {
			case <-done:
				if len(rs.blockCh) == 0 {
					return rs.state, errors.New("rollup sync early closed")
				}
			default:
			}

		case <-ctx.Done():
			return rs.state, ctx.Err()
		case blockInfo := <-rs.blockCh:
			if blockInfo.Block != nil {
				block := blockInfo.Block
				if rs.state.LastBlockHeight+1 > block.Height {
					continue
				} else if rs.state.LastBlockHeight+1 < block.Height {
					return rs.state, fmt.Errorf("sync height mismatch; expected %d, got %d", rs.state.LastBlockHeight+1, block.Height)
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

				// don't need to save seen commit here, seen commit is used only in consensus.
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

				err := rs.blockExec.Store().SetRollupSyncBatchChainHeight(0)
				if err != nil {
					return rs.state, err
				}
			} else if err := rs.blockExec.Store().SetRollupSyncBatchChainHeight(blockInfo.BatchChainHeight); err != nil {
				return rs.state, err
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
