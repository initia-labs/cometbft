package commands

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	dbm "github.com/cometbft/cometbft-db"

	cmtcfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/state"
	indexerv2 "github.com/cometbft/cometbft/state/indexer_v2"
	blockidxkvv2 "github.com/cometbft/cometbft/state/indexer_v2/block/kv"
	"github.com/cometbft/cometbft/state/txindex"
	kvv2 "github.com/cometbft/cometbft/state/txindex/kv_v2"
	"github.com/cometbft/cometbft/store"
)

const (
	reindexFailed = "event re-index failed: "
)

var (
	ErrHeightNotAvailable = errors.New("height is not available")
	ErrInvalidRequest     = errors.New("invalid request")
)

// ReIndexEventCmd constructs a command to re-index events in a block height interval.
var ReIndexEventCmd = &cobra.Command{
	Use:     "reindex-event",
	Aliases: []string{"reindex_event"},
	Short:   "reindex events to the event store backends",
	Long: `
reindex-event is an offline tooling to re-index block and tx events to the eventsinks,
you can run this command when the event store backend dropped/disconnected or you want to
replace the backend. The default start-height is 0, meaning the tooling will start
reindex from the base block height(inclusive); and the default end-height is 0, meaning
the tooling will reindex until the latest block height(inclusive). User can omit
either or both arguments.

Note: This operation requires ABCI Responses. Do not set DiscardABCIResponses to true if you
want to use this command.
	`,
	Example: `
	cometbft reindex-event
	cometbft reindex-event --start-height 2
	cometbft reindex-event --end-height 10
	cometbft reindex-event --start-height 2 --end-height 10
	`,
	Run: func(cmd *cobra.Command, args []string) {
		config, err := ParseConfig(cmd)
		if err != nil {
			fmt.Println(reindexFailed, err)
			return
		}
		bs, ss, err := loadStateAndBlockStore(config)
		if err != nil {
			fmt.Println(reindexFailed, err)
			return
		}

		state, err := ss.Load()
		if err != nil {
			fmt.Println(reindexFailed, err)
			return
		}

		if err := checkValidHeight(bs); err != nil {
			fmt.Println(reindexFailed, err)
			return
		}

		bi, ti, err := loadEventSinks(config, state.ChainID, bs, ss)
		if err != nil {
			fmt.Println(reindexFailed, err)
			return
		}

		riArgs := eventReIndexArgs{
			startHeight:  startHeight,
			endHeight:    endHeight,
			blockIndexer: bi,
			txIndexer:    ti,
			blockStore:   bs,
			stateStore:   ss,
			retainHeight: config.TxIndex.RetainHeight,
		}
		if err := eventReIndex(cmd, riArgs); err != nil {
			panic(fmt.Errorf("%s: %w", reindexFailed, err))
		}

		fmt.Println("event re-index finished")
	},
}

var (
	startHeight int64
	endHeight   int64
)

func init() {
	ReIndexEventCmd.Flags().Int64Var(&startHeight, "start-height", 0, "the block height would like to start for re-index")
	ReIndexEventCmd.Flags().Int64Var(&endHeight, "end-height", 0, "the block height would like to finish for re-index")
}

func loadEventSinks(cfg *cmtcfg.Config, chainID string, blockStore *store.BlockStore, stateStore state.Store) (indexerv2.BlockIndexer, txindex.TxIndexerV2, error) {
	switch strings.ToLower(cfg.TxIndex.Indexer) {
	case "null":
		return nil, nil, errors.New("found null event sink, please check the tx-index section in the config.toml")
	case "kv":
		store, err := dbm.NewDB("tx_index_v2", dbm.BackendType(cfg.DBBackend), cfg.DBDir())
		if err != nil {
			return nil, nil, err
		}

		txIndexer := kvv2.NewTxIndex(store, blockStore, stateStore, cfg.TxIndex.RetainHeight)
		blockIndexer := blockidxkvv2.New(dbm.NewPrefixDB(store, []byte("block_events")), blockStore, stateStore, cfg.TxIndex.RetainHeight)
		return blockIndexer, txIndexer, nil
	default:
		return nil, nil, fmt.Errorf("unsupported event sink type: %s", cfg.TxIndex.Indexer)
	}
}

type eventReIndexArgs struct {
	startHeight  int64
	endHeight    int64
	blockIndexer indexerv2.BlockIndexer
	txIndexer    txindex.TxIndexerV2
	blockStore   state.BlockStore
	stateStore   state.Store
	retainHeight int64
}

func eventReIndex(cmd *cobra.Command, args eventReIndexArgs) error {
	reindexFunc, err := txindex.ReindexEvents(cmd.Context(), log.NewTMLogger(log.NewSyncWriter(os.Stdout)), &cmtcfg.TxIndexConfig{
		Indexer:      "kv",
		RetainHeight: args.retainHeight,
	}, args.blockStore, args.stateStore, args.blockIndexer, args.txIndexer, args.startHeight, args.endHeight)
	if err != nil {
		return err
	}
	reindexFunc()
	return nil
}

func checkValidHeight(bs state.BlockStore) error {
	base := bs.Base()

	if startHeight == 0 {
		startHeight = base
		fmt.Printf("set the start block height to the base height of the blockstore %d \n", base)
	}

	if startHeight < base {
		return fmt.Errorf("%s (requested start height: %d, base height: %d)",
			ErrHeightNotAvailable, startHeight, base)
	}

	height := bs.Height()

	if startHeight > height {
		return fmt.Errorf(
			"%s (requested start height: %d, store height: %d)", ErrHeightNotAvailable, startHeight, height)
	}

	if endHeight == 0 || endHeight > height {
		endHeight = height
		fmt.Printf("set the end block height to the latest height of the blockstore %d \n", height)
	}

	if endHeight < base {
		return fmt.Errorf(
			"%s (requested end height: %d, base height: %d)", ErrHeightNotAvailable, endHeight, base)
	}

	if endHeight < startHeight {
		return fmt.Errorf(
			"%s (requested the end height: %d is less than the start height: %d)",
			ErrInvalidRequest, startHeight, endHeight)
	}

	return nil
}
