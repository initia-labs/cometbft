package rollupsync

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/rollupsync/provider"
	rstypes "github.com/cometbft/cometbft/rollupsync/types"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
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
	syncMode  rstypes.SyncMode

	l1Provider *provider.L1Provider

	batchCh chan rstypes.BatchChanInfo
	blockCh chan rstypes.BlockChanInfo
}

func NewRollupSyncer(cfg *config.RollupSyncConfig, logger log.Logger, state sm.State, blockExec *sm.BlockExecutor, store *store.BlockStore, proxyApp proxy.AppConns, syncMode rstypes.SyncMode) (*RollupSyncer, error) {
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
		syncMode:  syncMode,

		l1Provider: l1Provider,

		batchCh: make(chan rstypes.BatchChanInfo, 100),
		blockCh: make(chan rstypes.BlockChanInfo, 1000),
	}, nil
}

func (rs *RollupSyncer) Start(ctx context.Context) (stateResult sm.State, err error) {
	ctx, done := context.WithCancel(ctx)
	errGrp, ctx := errgroup.WithContext(ctx)

	// fetch last finalized block height
	targetL2BlockHeight, err := rs.l1Provider.GetLastFinalizedBlock(ctx)
	if err != nil {
		return sm.State{}, err
	}
	rs.targetBlockHeight = targetL2BlockHeight

	// if the target block height is already reached, return the current state
	if rs.state.LastBlockHeight >= int64(targetL2BlockHeight) {
		return rs.state, err
	}

	rs.logger.Info("start rollup sync", "initialHeight", rs.state.LastBlockHeight+1, "target", targetL2BlockHeight)

	errGrp.Go(func() (err error) {
		defer func() {
			rs.logger.Info("batch fetcher stopped")
		}()
		return rs.batchFetcher(ctx)
	})

	errGrp.Go(func() (err error) {
		defer func() {
			rs.logger.Info("batch processor stopped")
		}()
		return rs.batchProcessor(ctx, targetL2BlockHeight)
	})

	errGrp.Go(func() (err error) {
		defer func() {
			rs.logger.Info("block processor stopped")
			done()
		}()
		stateResult, err = rs.blockProcessor(ctx)
		return err
	})

	return stateResult, errGrp.Wait()
}
