package txindex_test

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	db "github.com/cometbft/cometbft-db"

	"github.com/cometbft/cometbft/libs/log"
	blockidxkv "github.com/cometbft/cometbft/state/indexer/block/kv"
	blockidxkvv2 "github.com/cometbft/cometbft/state/indexer_v2/block/kv"
	"github.com/cometbft/cometbft/state/txindex"
	kv "github.com/cometbft/cometbft/state/txindex/kv"
	kvv2 "github.com/cometbft/cometbft/state/txindex/kv_v2"
	"github.com/cometbft/cometbft/types"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmtstore "github.com/cometbft/cometbft/proto/tendermint/store"
	prototypes "github.com/cometbft/cometbft/proto/tendermint/types"
	sm "github.com/cometbft/cometbft/state"
	bstore "github.com/cometbft/cometbft/store"
)

func TestIndexerServiceIndexesBlocks(t *testing.T) {
	// event bus
	eventBus := types.NewEventBus()
	eventBus.SetLogger(log.TestingLogger())
	err := eventBus.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := eventBus.Stop(); err != nil {
			t.Error(err)
		}
	})

	store := db.NewMemDB()
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 1,
	}, db.NewPrefixDB(store, []byte("block_store")))
	blockStore := bstore.NewBlockStore(db.NewPrefixDB(store, []byte("block_store")))
	stateStore := sm.NewStore(db.NewPrefixDB(store, []byte("state_store")), sm.StoreOptions{})

	storeLegacy := db.NewMemDB()
	txIndexer := kv.NewTxIndex(storeLegacy, 0)
	blockIndexer := blockidxkv.New(db.NewPrefixDB(storeLegacy, []byte("block_events")), 0)

	// tx indexer
	txIndexerV2 := kvv2.NewTxIndex(store, blockStore, stateStore, 0)
	blockIndexerV2 := blockidxkvv2.New(db.NewPrefixDB(store, []byte("block_events")), blockStore, nil, 0)

	service := txindex.NewIndexerService(txIndexer, txIndexerV2, blockIndexer, blockIndexerV2, eventBus, false)
	service.SetLogger(log.TestingLogger())
	err = service.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := service.Stop(); err != nil {
			t.Error(err)
		}
	})

	// publish block with events
	err = eventBus.PublishEventNewBlockEvents(types.EventDataNewBlockEvents{
		Height: 1,
		Events: []abcitypes.Event{
			{
				Type: "begin_event",
				Attributes: []abcitypes.EventAttribute{
					{
						Key:   "proposer",
						Value: "FCAA001",
						Index: true,
					},
				},
			},
		},
		NumTxs: int64(2),
	})
	require.NoError(t, err)

	txResult1 := &abcitypes.TxResult{
		Height: 1,
		Index:  uint32(0),
		Tx:     types.Tx("foo"),
		Result: abcitypes.ExecTxResult{Code: 0},
	}
	txResult2 := &abcitypes.TxResult{
		Height: 1,
		Index:  uint32(1),
		Tx:     types.Tx("bar"),
		Result: abcitypes.ExecTxResult{Code: 0},
	}

	err = stateStore.SaveFinalizeBlockResponse(1, &abcitypes.ResponseFinalizeBlock{
		Events: []abcitypes.Event{},
		TxResults: []*abcitypes.ExecTxResult{
			&txResult1.Result,
			&txResult2.Result,
		},
		ValidatorUpdates:      []abcitypes.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	err = eventBus.PublishEventTx(types.EventDataTx{TxResult: *txResult1})
	require.NoError(t, err)

	err = eventBus.PublishEventTx(types.EventDataTx{TxResult: *txResult2})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	res, err := txIndexerV2.Get(types.Tx("foo").Hash())
	require.NoError(t, err)
	require.Equal(t, txResult1, res)

	ok, err := blockIndexerV2.Has(1)
	require.NoError(t, err)
	require.True(t, ok)

	res, err = txIndexerV2.Get(types.Tx("bar").Hash())
	require.NoError(t, err)
	require.Equal(t, txResult2, res)
}

func TestDisassembleMoveEvent(t *testing.T) {
	finalizedBlockResponse := &abcitypes.ResponseFinalizeBlock{
		Events: []abcitypes.Event{
			{
				Type: "move",
				Attributes: []abcitypes.EventAttribute{
					{
						Key:   "type_tag",
						Value: "0x1::BasicCoin::MintEvent",
					},
					{
						Key:   "data",
						Value: "{\"account\":\"0x2\",\"amount\":\"200\",\"coin_type\":\"0x1::BasicCoin::Initia\"}",
					},
				},
			},
		},
		TxResults: []*abcitypes.ExecTxResult{
			{
				Events: []abcitypes.Event{
					{
						Type: "move",
						Attributes: []abcitypes.EventAttribute{
							{
								Key:   "type_tag",
								Value: "0x1::BasicCoin::MintEvent",
							},
							{
								Key:   "data",
								Value: "{\"account\":\"0x2\",\"amount\":\"200\",\"coin_type\":\"0x1::BasicCoin::Initia\"}",
							},
						},
					},
				},
			},
		},
	}

	changed := txindex.DisassembleMoveEvent(finalizedBlockResponse)
	require.True(t, changed)

	require.Equal(t, finalizedBlockResponse.Events[0].Type, "move")

	require.True(t, slices.ContainsFunc(finalizedBlockResponse.Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "type_tag" && attr.Value == "0x1::BasicCoin::MintEvent"
	}))
	require.True(t, slices.ContainsFunc(finalizedBlockResponse.Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "account" && attr.Value == "0x2"
	}))
	require.True(t, slices.ContainsFunc(finalizedBlockResponse.Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "amount" && attr.Value == "200"
	}))
	require.True(t, slices.ContainsFunc(finalizedBlockResponse.Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "coin_type" && attr.Value == "0x1::BasicCoin::Initia"
	}))

	require.Equal(t, finalizedBlockResponse.TxResults[0].Events[0].Type, "move")

	require.True(t, slices.ContainsFunc(finalizedBlockResponse.TxResults[0].Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "type_tag" && attr.Value == "0x1::BasicCoin::MintEvent"
	}))
	require.True(t, slices.ContainsFunc(finalizedBlockResponse.TxResults[0].Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "account" && attr.Value == "0x2"
	}))
	require.True(t, slices.ContainsFunc(finalizedBlockResponse.TxResults[0].Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "amount" && attr.Value == "200"
	}))
	require.True(t, slices.ContainsFunc(finalizedBlockResponse.TxResults[0].Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "coin_type" && attr.Value == "0x1::BasicCoin::Initia"
	}))

	unchangedFinalizedBlockResponse := &abcitypes.ResponseFinalizeBlock{
		Events: []abcitypes.Event{
			{
				Type: "move",
				Attributes: []abcitypes.EventAttribute{
					{
						Key:   "type_tag",
						Value: "0x1::BasicCoin::MintEvent",
					},
					{
						Key:   "data",
						Value: "\"account\":\"0x2\",\"amount\":\"200\",\"coin_type\":\"0x1::BasicCoin::Initia\"}",
					},
				},
			},
		},
		TxResults: []*abcitypes.ExecTxResult{
			{
				Events: []abcitypes.Event{
					{
						Type: "move",
						Attributes: []abcitypes.EventAttribute{
							{
								Key:   "type_tag",
								Value: "0x1::BasicCoin::MintEvent",
							},
							{
								Key:   "adata",
								Value: "{\"account\":\"0x2\",\"amount\":\"200\",\"coin_type\":\"0x1::BasicCoin::Initia\"}",
							},
						},
					},
				},
			},
		},
	}

	changed = txindex.DisassembleMoveEvent(unchangedFinalizedBlockResponse)
	require.False(t, changed)

	require.Equal(t, unchangedFinalizedBlockResponse.Events[0].Type, "move")

	require.True(t, slices.ContainsFunc(unchangedFinalizedBlockResponse.Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "type_tag" && attr.Value == "0x1::BasicCoin::MintEvent"
	}))
	require.True(t, slices.ContainsFunc(unchangedFinalizedBlockResponse.Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "data" && attr.Value == "\"account\":\"0x2\",\"amount\":\"200\",\"coin_type\":\"0x1::BasicCoin::Initia\"}"
	}))

	require.Equal(t, unchangedFinalizedBlockResponse.TxResults[0].Events[0].Type, "move")

	require.True(t, slices.ContainsFunc(unchangedFinalizedBlockResponse.TxResults[0].Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "type_tag" && attr.Value == "0x1::BasicCoin::MintEvent"
	}))
	require.True(t, slices.ContainsFunc(unchangedFinalizedBlockResponse.TxResults[0].Events[0].Attributes, func(attr abcitypes.EventAttribute) bool {
		return attr.Key == "adata" && attr.Value == "{\"account\":\"0x2\",\"amount\":\"200\",\"coin_type\":\"0x1::BasicCoin::Initia\"}"
	}))
}
