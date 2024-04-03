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

	"github.com/cosmos/gogoproto/proto"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
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

	batchCh chan rstypes.BatchInfo
	blockCh chan rstypes.BlockInfo

	done chan struct{}
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

		batchCh: make(chan rstypes.BatchInfo, 100),
		blockCh: make(chan rstypes.BlockInfo, 1000),
	}, nil
}

func (rs *RollupSyncer) Start(ctx context.Context) (sm.State, error) {
	targetBlockHeight, err := rs.l1Provider.GetLatestFinalizedBlock(ctx)
	if err != nil {
		return sm.State{}, err
	}
	rs.logger.Info("Start rollup sync", "initialHeight", rs.state.LastBlockHeight+1, "target", targetBlockHeight)
	rs.targetBlockHeight = targetBlockHeight
	if rs.state.LastBlockHeight >= int64(targetBlockHeight) {
		return rs.state, err
	}

	batchInfoUpdates, err := rs.l1Provider.GetBatchInfoUpdates(ctx)
	if err != nil {
		return sm.State{}, err
	}
	batchInfoUpdates[len(batchInfoUpdates)-1].End = int64(targetBlockHeight)
	rs.logger.Info("Batchinfo updates", "history", batchInfoUpdates.String())

	return rs.blockSync(ctx, rs.state, batchInfoUpdates)
}

func (rs RollupSyncer) GetBatchProvider(chain string) (rstypes.BatchProvider, error) {
	switch chain {
	case rstypes.CHAIN_NAME_L1:
		return rs.l1Provider, nil
	case rstypes.CHAIN_NAME_CELESTIA:
		return provider.NewCelestiaProvider(rs.logger.With("provider", rstypes.CHAIN_NAME_CELESTIA), rs.cfg)
	}

	return nil, errors.New("not implemented")
}

func (rs *RollupSyncer) provideBatches(ctx context.Context, batchInfoUpdates rstypes.BatchInfoUpdates, height int64) error {
	for _, batchInfoUpdate := range batchInfoUpdates {
		if batchInfoUpdate.Start < height {
			continue
		}

		batchProvider, err := rs.GetBatchProvider(batchInfoUpdate.Chain)
		if err != nil {
			return err
		}
		batchProvider.SetSubmitter(batchInfoUpdate.Submitter)
		rs.logger.Info("update batch info", "height", batchInfoUpdate.Start, "chain", batchInfoUpdate.Chain, "submitter", batchInfoUpdate.Submitter)

		batchChainStart := int64(0)
		if batchInfoUpdate.Start <= rs.state.LastBlockHeight+1 {
			batchChainStart, _ = rs.blockExec.Store().GetRollupSyncBatchChainHeight()
			batchChainStart++
		}

		batchChainEnd, err := batchProvider.GetLastHeight(ctx)
		if err != nil {
			return err
		}
		rs.logger.Info("batch chain query range", "chain", batchInfoUpdate.Chain, "range", fmt.Sprintf("%d ~ %d", batchChainStart, batchChainEnd))

		err = rs.batchSync(ctx, batchInfoUpdate.End, batchProvider, batchChainStart, batchChainEnd)
		if err != nil {
			return err
		}

		height = batchInfoUpdate.End + 1
	}

	return nil
}

func (rs *RollupSyncer) batchSync(ctx context.Context, targetL2Height int64, batchProvider rstypes.BatchProvider, batchChainStart int64, batchChainEnd int64) error {
	bpCtx, cancelProvider := context.WithCancel(ctx)
	defer cancelProvider()

	done := make(chan struct{})
	go func() {
		err := batchProvider.BatchFetcher(bpCtx, rs.batchCh, batchChainStart, batchChainEnd)
		if err != nil {
			rs.logger.Error("batch provider", "fetcher", err.Error())
		}
		close(done)
	}()

	batch := make([]byte, 0, rs.cfg.MaxBatchBytes*rs.cfg.MaxBatchChunk)
	chunks := 0
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
				rs.blockCh <- rstypes.BlockInfo{
					BatchChainHeight: batchInfo.BatchChainHeight,
				}
				continue BATCH_LOOP
			}
			rs.logger.Debug("Receive a batch chunk")

			chunks++
			batch = append(batch, batchInfo.Batch...)
			rawData, err := DecompressBatch(batch)

			if err != nil {
				if chunks >= int(rs.cfg.MaxBatchChunk) {
					return err
				}
				continue BATCH_LOOP
			}
			batch = batch[:0]
			chunks = 0

			dataLength := len(rawData)
			rawBlocks := rawData[:dataLength-1]
			rawCommit := rawData[dataLength-1]

			for i, blockBytes := range rawBlocks {
				pbb := new(cmtproto.Block)
				err := proto.Unmarshal(blockBytes, pbb)
				if err != nil {
					return err
				}

				block, err := types.BlockFromProto(pbb)
				if err != nil {
					return err
				}
				rs.blockCh <- rstypes.BlockInfo{
					Block: block,
				}

				if i == len(rawBlocks)-1 {
					pbc := new(cmtproto.Commit)
					err = proto.Unmarshal(rawCommit, pbc)
					if err != nil {
						return err
					}

					commit, err := types.CommitFromProto(pbc)
					if err != nil {
						return err
					}
					rs.blockCh <- rstypes.BlockInfo{
						Commit: commit,
					}
				}

				if block.Height == targetL2Height {
					break BATCH_LOOP
				}
			}
		}
	}
	rs.logger.Info("Completed fetching batches")
	return nil
}

func (rs *RollupSyncer) blockSync(ctx context.Context, state sm.State, batchInfoUpdates rstypes.BatchInfoUpdates) (sm.State, error) {
	batchCtx, cancelBatchSync := context.WithCancel(ctx)
	defer cancelBatchSync()

	done := make(chan struct{})
	go func() {
		err := rs.provideBatches(batchCtx, batchInfoUpdates, state.LastBlockHeight+1)
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
					return state, errors.New("rollup sync early closed")
				}
			default:
			}

		case <-ctx.Done():
			return state, ctx.Err()
		case blockInfo := <-rs.blockCh:
			block := blockInfo.Block
			if block != nil {
				if state.LastBlockHeight+1 > block.Height {
					continue LOOP
				} else if state.LastBlockHeight+1 < block.Height {
					return state, fmt.Errorf("sync height mismatch; expected %d, got %d", state.LastBlockHeight+1, block.Height)
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

				// don't need to save seen commit here, seen commit is used only in consensus.
				rs.store.SaveBlock(block, blockParts, nil)

				state, err = rs.blockExec.ApplyBlock(state, blockID, block)
				if err != nil {
					return state, err
				}
			} else if blockInfo.Commit != nil {
				lastCommit = blockInfo.Commit
				if state.LastBlockHeight == int64(rs.targetBlockHeight) {
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
