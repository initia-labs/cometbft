package block

import (
	dbm "github.com/cometbft/cometbft-db"

	"github.com/cometbft/cometbft/config"
	sm "github.com/cometbft/cometbft/state"
	indexerv2 "github.com/cometbft/cometbft/state/indexer_v2"
	blockidxkv "github.com/cometbft/cometbft/state/indexer_v2/block/kv"
	blockidxnull "github.com/cometbft/cometbft/state/indexer_v2/block/null"
	"github.com/cometbft/cometbft/state/txindex"
	txkvv2 "github.com/cometbft/cometbft/state/txindex/kv_v2"
	"github.com/cometbft/cometbft/state/txindex/null"
	"github.com/cometbft/cometbft/store"
)

// IndexerFromConfig constructs a slice of indexer.EventSink using the provided
// configuration.
func IndexerFromConfig(cfg *config.Config, dbProvider config.DBProvider, chainID string) (
	filtermapTxIdx txindex.TxIndexerV2, filtermapBlockIdx indexerv2.BlockIndexer, err error,
) {
	filtermapTxIdx, filtermapBlockIdx, _, err = IndexerFromConfigWithDisabledIndexers(cfg, nil, nil, dbProvider, chainID)
	return filtermapTxIdx, filtermapBlockIdx, err
}

// IndexerFromConfigWithDisabledIndexers constructs a slice of indexer.EventSink using the provided
// configuration. If all indexers are disabled in the configuration, it returns null indexers.
// Otherwise, it creates the appropriate indexers based on the configuration.
func IndexerFromConfigWithDisabledIndexers(cfg *config.Config, blockStore *store.BlockStore, stateStore sm.Store, dbProvider config.DBProvider, chainID string) (
	filtermapTxIdx txindex.TxIndexerV2, filtermapBlockIdx indexerv2.BlockIndexer, allIndexersDisabled bool, err error,
) {
	switch cfg.TxIndex.Indexer {
	case "kv":
		store, err := dbProvider(&config.DBContext{ID: "tx_index_v2", Config: cfg})
		if err != nil {
			return nil, nil, false, err
		}

		return txkvv2.NewTxIndex(store, blockStore, stateStore, cfg.TxIndex.RetainHeight),
			blockidxkv.New(dbm.NewPrefixDB(store, []byte("block_events")), blockStore, stateStore, cfg.TxIndex.RetainHeight),
			false,
			nil

	default:
		return &null.TxIndexV2{}, &blockidxnull.BlockerIndexer{}, true, nil
	}
}
