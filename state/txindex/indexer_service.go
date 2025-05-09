package txindex

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/service"
	"github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/state/indexer"
	indexerv2 "github.com/cometbft/cometbft/state/indexer_v2"
	"github.com/cometbft/cometbft/types"

	abcitypes "github.com/cometbft/cometbft/abci/types"
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

		blockIdxPruningRunningV2 := atomic.Bool{}
		blockIdxPruningRunningV2.Store(false)

		txIdxPruningRunning := atomic.Bool{}
		txIdxPruningRunning.Store(false)

		txIdx2PruningRunning := atomic.Bool{}
		txIdx2PruningRunning.Store(false)

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
				} else {
					is.Logger.Info("indexed block events v2", "height", height)
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
					is.Logger.Debug("indexed transactions", "height", height, "num_txs", numTxs)
				}

				if err = is.txIdxrV2.AddBatch(batch); err != nil {
					is.Logger.Error("failed to index block txs v2", "height", height, "err", err)
					if is.terminateOnError {
						if err := is.Stop(); err != nil {
							is.Logger.Error("failed to stop", "err", err)
						}
						return
					}
				} else {
					is.Logger.Debug("indexed transactions v2", "height", height, "num_txs", numTxs)
				}

				if running := blockIdxPruningRunning.Swap(true); !running {
					go func() {
						defer blockIdxPruningRunning.Store(false)
						if err := is.blockIdxr.Prune(height); err != nil {
							is.Logger.Error("failed to prune tx index", "height", height, "err", err)
						}

						is.Logger.Debug("pruned block_index", "height", height)
					}()
				}

				if running := blockIdxPruningRunningV2.Swap(true); !running {
					go func() {
						defer blockIdxPruningRunningV2.Store(false)
						if err := is.blockIdxrV2.Prune(height); err != nil {
							is.Logger.Error("failed to prune tx index", "height", height, "err", err)
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
							is.Logger.Error("failed to prune tx index", "height", height, "err", err)
						}

						is.Logger.Debug("pruned tx_index", "height", height)
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

////////////////////////////////////////////////////
// Reindex events //////////////////////////////////
////////////////////////////////////////////////////

// ReindexEvents reindexes the events for the given height range.
func ReindexEvents(
	ctx context.Context,
	logger log.Logger,
	config *cfg.TxIndexConfig,
	blockStore state.BlockStore,
	stateStore state.Store,
	blockIndexerV2 indexerv2.BlockIndexer,
	txIndexerV2 TxIndexerV2,
	endHeight int64,
) (func(), error) {
	blockIndexerV2.StartMigration()
	txIndexerV2.StartMigration()

	startHeight := int64(1)
	baseHeight := blockStore.Base()
	storeHeight := blockStore.Height()
	if config.V2Migration.StartHeight == 0 {
		txLastSavedMigrationHeight, err := txIndexerV2.MigrationHeight()
		if err != nil {
			return nil, err
		}

		blockLastSavedMigrationHeight, err := blockIndexerV2.MigrationHeight()
		if err != nil {
			return nil, err
		}
		lastSavedMigrationHeight := min(txLastSavedMigrationHeight, blockLastSavedMigrationHeight)

		startHeight = max(baseHeight, lastSavedMigrationHeight+1)
	} else {
		startHeight = max(baseHeight, config.V2Migration.StartHeight)
	}

	if endHeight > 0 {
		endHeight = min(endHeight, storeHeight)
	} else {
		endHeight = storeHeight
	}

	if config.RetainHeight > 0 {
		minRetainHeight := storeHeight - config.RetainHeight + 1
		startHeight = max(startHeight, minRetainHeight)
		endHeight = max(endHeight, minRetainHeight)
	}

	// if the start height is greater than the end height, return a no-op function
	if startHeight > endHeight {
		return func() {}, nil
	}

	logger.Info("start re-indexing events", "startHeight", startHeight, "endHeight", endHeight)
	return func() {
		total := endHeight - startHeight + 1
		printHeight := startHeight + total/100
		for height := startHeight; height <= endHeight; height++ {
			select {
			case <-ctx.Done():
				return
			default:
				if err := reindexEvents(height, blockStore, stateStore, blockIndexerV2, txIndexerV2); err != nil {
					logger.Error("event re-index failed", "height", height, "error", err)
					return
				}
			}
			if height == printHeight {
				logger.Info("re-indexing events", "start", startHeight, "end", endHeight, "height", height, "progress", fmt.Sprintf("%d%%", (height-startHeight+1)*100/total), "total", total)
				printHeight += total / 100
			}
		}

		// update the last section bloom
		err := blockIndexerV2.FinishMigration(endHeight)
		if err != nil {
			logger.Error("failed to finalize block index re-index", "height", endHeight, "err", err)
		}

		err = txIndexerV2.FinishMigration(endHeight)
		if err != nil {
			logger.Error("failed to finalize tx index re-index", "height", endHeight, "err", err)
		}

		logger.Info("re-indexing events completed")
	}, nil
}

// reindexEvents reindexes the events for the given height.
func reindexEvents(height int64, blockStore state.BlockStore, stateStore state.Store, blockIndexerV2 indexerv2.BlockIndexer, txIndexerV2 TxIndexerV2) error {
	block := blockStore.LoadBlock(height)
	if block == nil {
		// skip the block if it is not found
		return nil
	}

	resp, err := stateStore.LoadFinalizeBlockResponse(height)
	if err != nil {
		// skip the block if it is not found
		return nil
	}

	if modified := ReconstructMoveEvent(resp); modified {
		err := stateStore.SaveFinalizeBlockResponse(height, resp)
		if err != nil {
			return fmt.Errorf("not able to save ABCI Response at height %d to the statestore", height)
		}
	}

	e := types.EventDataNewBlockEvents{
		Height: height,
		Events: resp.Events,
	}

	numTxs := len(resp.TxResults)

	var batch *Batch
	if numTxs > 0 {
		batch = NewBatch(int64(numTxs))

		for idx, txResult := range resp.TxResults {
			tr := abcitypes.TxResult{
				Height: height,
				Index:  uint32(idx),
				Tx:     block.Txs[idx],
				Result: *txResult,
			}

			if err = batch.Add(&tr); err != nil {
				return fmt.Errorf("adding tx to batch: %w", err)
			}
		}

		if err := txIndexerV2.AddBatch(batch); err != nil {
			return fmt.Errorf("tx event re-index at height %d failed: %w", height, err)
		} else if err := txIndexerV2.SetMigrationHeight(height); err != nil {
			return fmt.Errorf("failed to set migration height: %w", err)
		}
	}

	// index the block events and set the migration height
	if err := blockIndexerV2.Index(e); err != nil {
		return fmt.Errorf("block event re-index at height %d failed: %w", height, err)
	} else if err := blockIndexerV2.SetMigrationHeight(height); err != nil {
		return fmt.Errorf("failed to set migration height: %w", err)
	}

	return nil
}

// ReconstructMoveEvent is a helper function to reconstruct the move event of the ResponseFinalizeBlock
func ReconstructMoveEvent(resp *abcitypes.ResponseFinalizeBlock) (modified bool) {
	modified = reconstructMoveEvent(resp.Events)
	for _, txResult := range resp.TxResults {
		modified = reconstructMoveEvent(txResult.Events) || modified
	}

	return
}

// reconstructMoveEvent is a helper function to reconstruct the move event of the events
func reconstructMoveEvent(events []abcitypes.Event) (modified bool) {
	if events == nil {
		return false
	}

	reconstructFunc := func(attrs []abcitypes.EventAttribute) []abcitypes.EventAttribute {
		for _, attr := range attrs {
			if attr.Key != "data" {
				continue
			}

			// if the attribute is a data event, disassemble it and add the new attributes to the attrs slice
			var dataEvent map[string]any
			err := json.Unmarshal([]byte(attr.Value), &dataEvent)
			if err != nil {
				continue
			}

			for k, v := range dataEvent {
				attrs = append(attrs, abcitypes.EventAttribute{
					Key:   k,
					Value: fmt.Sprintf("%v", v),
					Index: attr.Index,
				})
			}
		}
		return attrs
	}

	for eventIndex, event := range events {
		if event.Type != "move" {
			continue
		}

		if newAttributes := reconstructFunc(event.Attributes); len(newAttributes) != len(event.Attributes) {
			events[eventIndex].Attributes = newAttributes
			modified = true
		}
	}

	return
}
