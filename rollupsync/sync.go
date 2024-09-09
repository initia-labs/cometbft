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
	logger   log.Logger
	cfg      *config.RollupSyncConfig
	syncMode rstypes.SyncMode

	targetBlockHeight uint64

	// immutable
	state sm.State

	blockExec *sm.BlockExecutor
	store     *store.BlockStore
	proxyApp  proxy.AppConns

	l1Provider *provider.L1Provider

	batchChClosed chan struct{}
	batchCh       chan rstypes.BatchChanInfo
	blockChClosed chan struct{}
	blockCh       chan rstypes.BlockChanInfo
}

func NewRollupSyncer(cfg *config.RollupSyncConfig, logger log.Logger, state sm.State, blockExec *sm.BlockExecutor, store *store.BlockStore, proxyApp proxy.AppConns) (*RollupSyncer, error) {
	l1Provider, err := provider.NewL1Provider(logger, cfg)
	if err != nil {
		return nil, err
	}
	syncMode := rstypes.SyncModeFromString(cfg.Mode)

	return &RollupSyncer{
		logger:   logger,
		cfg:      cfg,
		syncMode: syncMode,

		state:     state,
		blockExec: blockExec,
		store:     store,
		proxyApp:  proxyApp,

		l1Provider: l1Provider,

		batchChClosed: make(chan struct{}),
		batchCh:       make(chan rstypes.BatchChanInfo, 100),
		blockChClosed: make(chan struct{}),
		blockCh:       make(chan rstypes.BlockChanInfo, 1000),
	}, nil
}

func (rs *RollupSyncer) Start(baseCtx context.Context) (sm.State, error) {
	errGrp, ctx := errgroup.WithContext(baseCtx)
	// fetch last finalized block height
	targetL2BlockHeight, err := rs.l1Provider.GetLastFinalizedBlock(ctx)
	if err != nil {
		return rs.state, err
	}
	rs.targetBlockHeight = targetL2BlockHeight

	// if the target block height is already reached, return the current state
	if rs.state.LastBlockHeight >= int64(targetL2BlockHeight) {
		return rs.state, err
	}

	rs.logger.Info("start rollup sync", "initialHeight", rs.state.LastBlockHeight+1, "target", targetL2BlockHeight, "mode", rs.syncMode.String())

	batchCtx, done := context.WithCancel(ctx)
	errGrp.Go(func() (err error) {
		defer func() {
			rs.logger.Info("block processor stopped")
			done()
		}()
		return rs.blockProcessor(ctx)
	})

	errGrp.Go(func() (err error) {
		defer func() {
			rs.logger.Info("batch fetcher stopped")
		}()
		return rs.batchFetcher(batchCtx)
	})

	errGrp.Go(func() (err error) {
		defer func() {
			rs.logger.Info("batch processor stopped")
		}()
		return rs.batchProcessor(batchCtx)
	})

	err = errGrp.Wait()
	if err != nil {
		return rs.state, err
	}

	select {
	case <-baseCtx.Done():
		return rs.state, baseCtx.Err()
	default:
	}

	return rs.state, nil
}
