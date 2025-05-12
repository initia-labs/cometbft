package kv

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	dbm "github.com/cometbft/cometbft-db"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	"github.com/cometbft/cometbft/types"

	"github.com/cometbft/cometbft/state/txindex"

	cmtstore "github.com/cometbft/cometbft/proto/tendermint/store"
	prototypes "github.com/cometbft/cometbft/proto/tendermint/types"
	sm "github.com/cometbft/cometbft/state"
	bstore "github.com/cometbft/cometbft/store"
)

func BenchmarkTxSearch(b *testing.B) {
	dbDir, err := os.MkdirTemp("", "benchmark_tx_search_test")
	if err != nil {
		b.Errorf("failed to create temporary directory: %s", err)
	}

	db, err := dbm.NewGoLevelDB("benchmark_tx_search_test", dbDir)
	if err != nil {
		b.Errorf("failed to create database: %s", err)
	}

	blockStoreDB := dbm.NewPrefixDB(db, []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 1000,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(dbm.NewPrefixDB(db, []byte("state_store")), sm.StoreOptions{})
	indexer := NewTxIndex(db, blockStore, stateStore, 0)

	for i := 0; i < 1000; i++ {
		events := []abci.Event{
			{
				Type: "transfer",
				Attributes: []abci.EventAttribute{
					{Key: "address", Value: fmt.Sprintf("address_%d", i%100), Index: true},
					{Key: "amount", Value: "50", Index: true},
				},
			},
		}

		txBz := make([]byte, 8)
		if _, err := rand.Read(txBz); err != nil {
			b.Errorf("failed produce random bytes: %s", err)
		}

		txResult := &abci.TxResult{
			Height: int64(i),
			Index:  0,
			Tx:     types.Tx(string(txBz)),
			Result: abci.ExecTxResult{
				Data:   []byte{0},
				Code:   abci.CodeTypeOK,
				Log:    "",
				Events: events,
			},
		}

		err = stateStore.SaveFinalizeBlockResponse(int64(i), &abci.ResponseFinalizeBlock{
			Events: []abci.Event{},
			TxResults: []*abci.ExecTxResult{
				&txResult.Result,
			},
			ValidatorUpdates:      []abci.ValidatorUpdate{},
			ConsensusParamUpdates: &prototypes.ConsensusParams{},
			AppHash:               []byte("app_hash"),
		})
		if err != nil {
			b.Errorf("failed to save finalize block response: %s", err)
		}

		batch := txindex.NewBatch(1)
		batch.Ops[0] = txResult
		if err := indexer.AddBatch(batch); err != nil {
			b.Errorf("failed to index tx: %s", err)
		}
	}

	txQuery := query.MustCompile(`transfer.address = 'address_43' AND transfer.amount = 50`)

	b.ResetTimer()

	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		resultChan, errChan := indexer.Search(ctx, txQuery, 1000)

		var err error
	RESULT_LOOP:
		for {
			select {
			case _, ok := <-resultChan:
				if !ok {
					break RESULT_LOOP
				}
			case err = <-errChan:
				break RESULT_LOOP
			}
		}
		if err != nil {
			b.Errorf("failed to query for txs: %s", err)
		}
	}
}
