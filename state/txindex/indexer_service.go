package txindex

import (
	"context"
	"sync/atomic"

	"github.com/cometbft/cometbft/libs/service"
	"github.com/cometbft/cometbft/state/indexer"
	indexerv2 "github.com/cometbft/cometbft/state/indexer_v2"
	"github.com/cometbft/cometbft/types"
)

// XXX/TODO: These types should be moved to the indexer package.

const (
	subscriber = "IndexerService"
)

// IndexerService connects event bus, transaction and block indexers together in
// order to index transactions and blocks coming from the event bus.
type IndexerService struct {
	service.BaseService

	txIdxr           TxIndexer
	txIdxrV2         TxIndexerV2
	blockIdxr        indexer.BlockIndexer
	blockIdxrV2      indexerv2.BlockIndexer
	eventBus         *types.EventBus
	terminateOnError bool
}

// NewIndexerService returns a new service instance.
func NewIndexerService(
	txIdxr TxIndexer,
	txIdxrV2 TxIndexerV2,
	blockIdxr indexer.BlockIndexer,
	blockIdxrV2 indexerv2.BlockIndexer,
	eventBus *types.EventBus,
	terminateOnError bool,
) *IndexerService {
	is := &IndexerService{txIdxr: txIdxr, txIdxrV2: txIdxrV2, blockIdxr: blockIdxr, blockIdxrV2: blockIdxrV2, eventBus: eventBus, terminateOnError: terminateOnError}
	is.BaseService = *service.NewBaseService(nil, "IndexerService", is)
	return is
}

// OnStart implements service.Service by subscribing for all transactions
// and indexing them by events.
func (is *IndexerService) OnStart() error {
	// Use SubscribeUnbuffered here to ensure both subscriptions does not get
	// canceled due to not pulling messages fast enough. Cause this might
	// sometimes happen when there are no other subscribers.
	blockSub, err := is.eventBus.SubscribeUnbuffered(
		context.Background(),
		subscriber,
		types.EventQueryNewBlockEvents)
	if err != nil {
		return err
	}

	txsSub, err := is.eventBus.SubscribeUnbuffered(context.Background(), subscriber, types.EventQueryTx)
	if err != nil {
		return err
	}

	go func() {
		blockIdxPruningRunning := atomic.Bool{}
		blockIdxPruningRunning.Store(false)

		txIdxPruningRunning := atomic.Bool{}
		txIdxPruningRunning.Store(false)

		txIdx2PruningRunning := atomic.Bool{}
		txIdx2PruningRunning.Store(false)

		is.txIdxrV2.Start()
		is.blockIdxrV2.Start()

		for {
			select {
			case <-blockSub.Canceled():
				return
			case msg := <-blockSub.Out():
				eventNewBlockEvents := msg.Data().(types.EventDataNewBlockEvents)
				height := eventNewBlockEvents.Height
				numTxs := eventNewBlockEvents.NumTxs

				batch := NewBatch(numTxs)
				for i := int64(0); i < numTxs; i++ {
					msg2 := <-txsSub.Out()
					txResult := msg2.Data().(types.EventDataTx).TxResult

					if err = batch.Add(&txResult); err != nil {
						is.Logger.Error(
							"failed to add tx to batch",
							"height", height,
							"index", txResult.Index,
							"err", err,
						)

						if is.terminateOnError {
							if err := is.Stop(); err != nil {
								is.Logger.Error("failed to stop", "err", err)
							}
							return
						}
					}
				}

				if err := is.blockIdxr.Index(eventNewBlockEvents); err != nil {
					is.Logger.Error("failed to index block", "height", height, "err", err)
					if is.terminateOnError {
						if err := is.Stop(); err != nil {
							is.Logger.Error("failed to stop", "err", err)
						}
						return
					}
				} else {
					is.Logger.Info("indexed block events", "height", height)
				}

				if err := is.blockIdxrV2.Index(eventNewBlockEvents); err != nil {
					is.Logger.Error("failed to index block v2", "height", height, "err", err)
					if is.terminateOnError {
						if err := is.Stop(); err != nil {
							is.Logger.Error("failed to stop", "err", err)
						}
						return
					}
				}

				if err = is.txIdxr.AddBatch(batch); err != nil {
					is.Logger.Error("failed to index block txs", "height", height, "err", err)
					if is.terminateOnError {
						if err := is.Stop(); err != nil {
							is.Logger.Error("failed to stop", "err", err)
						}
						return
					}
				} else {
					is.Logger.Info("indexed transactions", "height", height, "num_txs", numTxs)
				}

				if err = is.txIdxrV2.AddBatch(batch, height); err != nil {
					is.Logger.Error("failed to index block txs v2", "height", height, "err", err)
					if is.terminateOnError {
						if err := is.Stop(); err != nil {
							is.Logger.Error("failed to stop", "err", err)
						}
						return
					}
				}

				if running := blockIdxPruningRunning.Swap(true); !running {
					go func() {
						defer blockIdxPruningRunning.Store(false)
						if err := is.blockIdxr.Prune(height); err != nil {
							is.Logger.Error("failed to prune block index", "height", height, "err", err)
						}

						is.Logger.Debug("pruned block_index", "height", height)
					}()
				}

				if running := txIdxPruningRunning.Swap(true); !running {
					go func() {
						defer txIdxPruningRunning.Store(false)
						if err := is.txIdxr.Prune(height); err != nil {
							is.Logger.Error("failed to prune tx index", "height", height, "err", err)
						}

						is.Logger.Debug("pruned tx_index", "height", height)
					}()
				}

				if running := txIdx2PruningRunning.Swap(true); !running {
					go func() {
						defer txIdx2PruningRunning.Store(false)
						if err := is.txIdxrV2.Prune(height); err != nil {
							is.Logger.Error("failed to prune tx index v2", "height", height, "err", err)
						}

						is.Logger.Debug("pruned tx_index v2", "height", height)
					}()
				}
			}
		}
	}()

	return nil
}

// OnStop implements service.Service by unsubscribing from all transactions.
func (is *IndexerService) OnStop() {
	if is.eventBus.IsRunning() {
		_ = is.eventBus.UnsubscribeAll(context.Background(), subscriber)
	}
}
