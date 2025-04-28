package kv

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cosmos/gogoproto/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/cometbft/cometbft-db"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	"github.com/cometbft/cometbft/state/txindex"
	"github.com/cometbft/cometbft/types"

	cmtrand "github.com/cometbft/cometbft/libs/rand"

	cmtstore "github.com/cometbft/cometbft/proto/tendermint/store"
	prototypes "github.com/cometbft/cometbft/proto/tendermint/types"
	sm "github.com/cometbft/cometbft/state"
	bstore "github.com/cometbft/cometbft/store"
)

func TestTxIndex(t *testing.T) {
	tx := types.Tx("HELLO WORLD")
	txResult := &abci.TxResult{
		Height: 1,
		Index:  0,
		Tx:     tx,
		Result: abci.ExecTxResult{
			Data: []byte{0},
			Code: abci.CodeTypeOK, Log: "", Events: nil,
		},
	}
	hash := tx.Hash()

	blockStoreDB := db.NewPrefixDB(db.NewMemDB(), []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 1,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	indexer := NewTxIndex(db.NewMemDB(), blockStore, stateStore, 0)

	batch := txindex.NewBatch(1)
	if err := batch.Add(txResult); err != nil {
		t.Error(err)
	}
	err = indexer.AddBatch(batch)
	require.NoError(t, err)

	loadedTxResult, err := indexer.Get(hash)
	require.NoError(t, err)
	assert.True(t, proto.Equal(txResult, loadedTxResult))

	tx2 := types.Tx("BYE BYE WORLD")
	txResult2 := &abci.TxResult{
		Height: 1,
		Index:  0,
		Tx:     tx2,
		Result: abci.ExecTxResult{
			Data: []byte{0},
			Code: abci.CodeTypeOK, Log: "", Events: nil,
		},
	}
	hash2 := tx2.Hash()

	err = stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult2.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	batch2 := txindex.NewBatch(1)
	err = batch2.Add(txResult2)
	require.NoError(t, err)
	err = indexer.AddBatch(batch2)
	require.NoError(t, err)

	loadedTxResult2, err := indexer.Get(hash2)
	require.NoError(t, err)
	assert.True(t, proto.Equal(txResult2, loadedTxResult2))
}

func TestTxSearch(t *testing.T) {
	txResult := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "1", Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "owner", Value: "/Ivan/", Index: true}}},
		{Type: "", Attributes: []abci.EventAttribute{{Key: "not_allowed", Value: "Vlad", Index: true}}},
	})
	hash := types.Tx(txResult.Tx).Hash()

	blockStoreDB := db.NewPrefixDB(db.NewMemDB(), []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 1,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	indexer := NewTxIndex(db.NewMemDB(), blockStore, stateStore, 0)

	batch := txindex.NewBatch(1)
	err = batch.Add(txResult)
	require.NoError(t, err)
	err = indexer.AddBatch(batch)
	require.NoError(t, err)

	testCases := []struct {
		q             string
		resultsLength int
		expectedError bool
	}{
		//	search by hash
		{fmt.Sprintf("tx.hash = '%X'", hash), 1, false},
		// search by hash (lower)
		{fmt.Sprintf("tx.hash = '%x'", hash), 1, false},
		// search by exact match (one key)
		{"account.number = 1", 1, false},
		// search by exact match (two keys)
		{"account.number = 1 AND account.owner = 'Ivan'", 0, false},
		{"account.owner = 'Ivan' AND account.number = 1", 0, false},
		{"account.owner = '/Ivan/'", 1, false},
		// search by exact match (two keys)
		{"account.number = 1 AND account.owner = 'Vlad'", 0, false},
		{"account.owner = 'Vlad' AND account.number = 1", 0, false},
		{"account.number >= 1 AND account.owner = 'Vlad'", 0, true},
		{"account.owner = 'Vlad' AND account.number >= 1", 0, true},
		{"account.number <= 0", 0, true},
		{"account.number <= 0 AND account.owner = 'Ivan'", 0, true},
		{"account.number < 10000 AND account.owner = 'Ivan'", 0, true},
		// search using a prefix of the stored value
		{"account.owner = 'Iv'", 0, false},
		// search by range
		{"account.number >= 1 AND account.number <= 5", 1, true},
		// search by range and another key
		{"account.number >= 1 AND account.owner = 'Ivan' AND account.number <= 5", 0, true},
		// search by range (lower bound)
		{"account.number >= 1", 1, true},
		// search by range (upper bound)
		{"account.number <= 5", 1, true},
		{"account.number <= 1", 1, true},
		// search using not allowed key
		{"not_allowed = 'boom'", 0, false},
		{"not_allowed = 'Vlad'", 0, false},
		// search for not existing tx result
		{"account.number >= 2 AND account.number <= 5 AND tx.height > 0", 0, true},
		// search using not existing key
		{"account.date >= TIME 2013-05-03T14:45:00Z", 0, true},
		// search using CONTAINS
		{"account.owner CONTAINS 'an'", 1, true},
		//	search for non existing value using CONTAINS
		{"account.owner CONTAINS 'Vlad'", 0, true},
		{"account.owner CONTAINS 'Ivann'", 0, true},
		{"account.owner CONTAINS 'IIvan'", 0, true},
		{"account.owner CONTAINS 'Iva n'", 0, true},
		{"account.owner CONTAINS ' Ivan'", 0, true},
		{"account.owner CONTAINS 'Ivan '", 0, true},
		// search using the wrong key (of numeric type) using CONTAINS
		{"account.number CONTAINS 'Iv'", 0, true},
		// search using EXISTS
		{"account.number EXISTS", 1, true},
		// search using EXISTS for non existing key
		{"account.date EXISTS", 0, true},
		{"not_allowed EXISTS", 0, true},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.q, func(t *testing.T) {
			resultChan, errChan := indexer.Search(ctx, query.MustCompile(tc.q), 1000)
			results := make([]abci.TxResult, 0)

			var err error
		RESULT_LOOP:
			for {
				select {
				case result, ok := <-resultChan:
					if !ok {
						break RESULT_LOOP
					}
					results = append(results, result)
				case err = <-errChan:
					break RESULT_LOOP
				}
			}
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, results, tc.resultsLength)
				if tc.resultsLength > 0 {
					for _, txr := range results {
						assert.Equal(t, txr.Height, txResult.Height)
						assert.Equal(t, txr.Index, txResult.Index)
					}
				}
			}
		})
	}
}

func TestTxSearchEventMatch(t *testing.T) {
	txResult := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "1", Index: true}, {Key: "owner", Value: "Ana", Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "2", Index: true}, {Key: "owner", Value: "/Ivan/.test", Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "3", Index: false}, {Key: "owner", Value: "Mickey", Index: false}}},
		{Type: "", Attributes: []abci.EventAttribute{{Key: "not_allowed", Value: "Vlad", Index: true}}},
	})

	blockStoreDB := db.NewPrefixDB(db.NewMemDB(), []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 1,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	indexer := NewTxIndex(db.NewMemDB(), blockStore, stateStore, 0)

	batch := txindex.NewBatch(1)
	err = batch.Add(txResult)
	require.NoError(t, err)
	err = indexer.AddBatch(batch)
	require.NoError(t, err)

	testCases := map[string]struct {
		q             string
		resultsLength int
		expectedError bool
	}{
		"Return all events from a height": {
			q:             "tx.height = 1",
			resultsLength: 1,
			expectedError: false,
		},
		"Don't match non-indexed events": {
			q:             "account.number = 3 AND account.owner = 'Mickey'",
			resultsLength: 0,
			expectedError: false,
		},
		"Return all events from a height with range": {
			q:             "tx.height > 0",
			resultsLength: 1,
			expectedError: false,
		},
		"Return all events from a height with range 2": {
			q:             "tx.height <= 1",
			resultsLength: 1,
			expectedError: false,
		},
		"Return all events from a height (deduplicate height)": {
			q:             "tx.height = 1 AND tx.height = 1",
			resultsLength: 0,
			expectedError: true,
		},
		"Match attributes with height range and event": {
			q:             "tx.height < 2 AND tx.height > 0 AND account.number > 0 AND account.number <= 1 AND account.owner CONTAINS 'Ana'",
			resultsLength: 0,
			expectedError: true,
		},
		"Match attributes with multiple CONTAIN and height range": {
			q:             "tx.height < 2 AND tx.height > 0 AND account.number = 1 AND account.owner CONTAINS 'Ana' AND account.owner CONTAINS 'An'",
			resultsLength: 0,
			expectedError: true,
		},
		"Match attributes with height range and event - no match": {
			q:             "tx.height < 2 AND tx.height > 0 AND account.number = 2 AND account.owner = 'Ana'",
			resultsLength: 1,
			expectedError: false,
		},
		"Match attributes with event": {
			q:             "account.number = 2 AND account.owner = 'Ana' AND tx.height = 1",
			resultsLength: 1,
			expectedError: false,
		},
		"Deduplication test - should return nothing if attribute repeats multiple times": {
			q:             "tx.height < 2 AND account.number = 3 AND account.number = 2 AND account.number = 5",
			resultsLength: 0,
			expectedError: false,
		},
		" Match range with special character": {
			q:             "account.number < 2 AND account.owner = '/Ivan/.test'",
			resultsLength: 0,
			expectedError: true,
		},
		" Match range with special character 2": {
			q:             "account.number <= 2 AND account.owner = '/Ivan/.test' AND tx.height > 0",
			resultsLength: 0,
			expectedError: true,
		},
		" Match range with contains with multiple items": {
			q:             "account.number <= 2 AND account.owner CONTAINS '/Iv' AND account.owner CONTAINS 'an' AND tx.height = 1",
			resultsLength: 0,
			expectedError: true,
		},
		" Match range with contains": {
			q:             "account.number <= 2 AND account.owner CONTAINS 'an' AND tx.height > 0",
			resultsLength: 0,
			expectedError: true,
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.q, func(t *testing.T) {
			resultChan, errChan := indexer.Search(ctx, query.MustCompile(tc.q), 1000)
			results := make([]abci.TxResult, 0)

			var err error
		RESULT_LOOP:
			for {
				select {
				case result, ok := <-resultChan:
					if !ok {
						break RESULT_LOOP
					}
					results = append(results, result)
				case err = <-errChan:
					break RESULT_LOOP
				}
			}
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, results, tc.resultsLength)
				if tc.resultsLength > 0 {
					for _, txr := range results {
						assert.Equal(t, txr.Height, txResult.Height)
						assert.Equal(t, txr.Index, txResult.Index)
					}
				}
			}
		})
	}
}

func TestTxSearchEventMatchByHeight(t *testing.T) {
	txResult := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "1", Index: true}, {Key: "owner", Value: "Ana", Index: true}}},
	})

	blockStoreDB := db.NewPrefixDB(db.NewMemDB(), []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 10,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	indexer := NewTxIndex(db.NewMemDB(), blockStore, stateStore, 0)

	batch := txindex.NewBatch(1)
	err = batch.Add(txResult)
	require.NoError(t, err)
	err = indexer.AddBatch(batch)
	require.NoError(t, err)

	txResult10 := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "1", Index: true}, {Key: "owner", Value: "/Ivan/.test", Index: true}}},
	})
	txResult10.Tx = types.Tx("HELLO WORLD 10")
	txResult10.Height = 10

	err = stateStore.SaveFinalizeBlockResponse(10, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult10.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	batch10 := txindex.NewBatch(1)
	err = batch10.Add(txResult10)
	require.NoError(t, err)
	err = indexer.AddBatch(batch10)
	require.NoError(t, err)

	testCases := map[string]struct {
		q             string
		resultsLength int
	}{
		"Return all events from a height 1": {
			q:             "tx.height = 1",
			resultsLength: 1,
		},
		"Return all events from a height 10": {
			q:             "tx.height = 10",
			resultsLength: 1,
		},
		"Return all events from a height 5": {
			q:             "tx.height = 5",
			resultsLength: 0,
		},
		"Return all events from a height in [2; 5]": {
			q:             "tx.height >= 2 AND tx.height <= 5",
			resultsLength: 0,
		},
		"Return all events from a height in [1; 5]": {
			q:             "tx.height >= 1 AND tx.height <= 5",
			resultsLength: 1,
		},
		"Return all events from a height in [1; 10]": {
			q:             "tx.height >= 1 AND tx.height <= 10",
			resultsLength: 2,
		},
		"Return all events from a height in [1; 5] by account.number": {
			q:             "tx.height >= 1 AND tx.height <= 5 AND account.number=1",
			resultsLength: 1,
		},
		"Return all events from a height in [1; 10] by account.number 2": {
			q:             "tx.height >= 1 AND tx.height <= 10 AND account.number=1",
			resultsLength: 2,
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.q, func(t *testing.T) {
			resultChan, errChan := indexer.Search(ctx, query.MustCompile(tc.q), 1000)
			results := make([]abci.TxResult, 0)

			var err error
		RESULT_LOOP:
			for {
				select {
				case result, ok := <-resultChan:
					if !ok {
						break RESULT_LOOP
					}
					results = append(results, result)
				case err = <-errChan:
					break RESULT_LOOP
				}
			}
			assert.NoError(t, err)
			assert.Len(t, results, tc.resultsLength)
			if tc.resultsLength > 0 {
				for _, txr := range results {
					if txr.Height == 1 {
						assert.Equal(t, txr.Height, txResult.Height)
						assert.Equal(t, txr.Index, txResult.Index)
					} else if txr.Height == 10 {
						assert.Equal(t, txr.Height, txResult10.Height)
						assert.Equal(t, txr.Index, txResult10.Index)
					} else {
						assert.True(t, false)
					}
				}
			}
		})
	}
}

func TestTxSearchWithCancelation(t *testing.T) {
	txResult := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "1", Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "owner", Value: "Ivan", Index: true}}},
		{Type: "", Attributes: []abci.EventAttribute{{Key: "not_allowed", Value: "Vlad", Index: true}}},
	})

	blockStoreDB := db.NewPrefixDB(db.NewMemDB(), []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 1,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	indexer := NewTxIndex(db.NewMemDB(), blockStore, stateStore, 0)

	batch := txindex.NewBatch(1)
	err = batch.Add(txResult)
	require.NoError(t, err)
	err = indexer.AddBatch(batch)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resultChan, errChan := indexer.Search(ctx, query.MustCompile(`account.number = 1`), 1000)
	results := make([]abci.TxResult, 0)

RESULT_LOOP:
	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				break RESULT_LOOP
			}
			results = append(results, result)
		case err = <-errChan:
			break RESULT_LOOP
		}
	}
	assert.NoError(t, err)
	assert.Empty(t, results)
}

func TestTxSearchOneTxWithMultipleSameTagsButDifferentValues(t *testing.T) {
	txResult := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "1", Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "2", Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "3", Index: false}}},
	})

	blockStoreDB := db.NewPrefixDB(db.NewMemDB(), []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 1,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	indexer := NewTxIndex(db.NewMemDB(), blockStore, stateStore, 0)

	batch := txindex.NewBatch(1)
	err = batch.Add(txResult)
	require.NoError(t, err)
	err = indexer.AddBatch(batch)
	require.NoError(t, err)

	testCases := []struct {
		name  string
		q     string
		found bool
	}{
		{
			q:     "account.number = 1",
			found: true,
		},
		{
			q:     "account.number = 3",
			found: false,
		},
		{
			q:     "account.number = 1 AND tx.height > 0",
			found: true,
		},
		{
			q:     "account.number = 2 AND tx.height = 1",
			found: true,
		},

		{
			q:     "account.number = 1 AND tx.height > 1",
			found: false,
		},

		{
			q:     "account.number = 1 AND tx.height = 3",
			found: false,
		},
		{
			q:     "account.number = 4",
			found: false,
		},
		{
			q:     "account.number = 'something'",
			found: false,
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		resultChan, errChan := indexer.Search(ctx, query.MustCompile(tc.q), 1000)
		results := make([]abci.TxResult, 0)

		var err error
	RESULT_LOOP:
		for {
			select {
			case result, ok := <-resultChan:
				if !ok {
					break RESULT_LOOP
				}
				results = append(results, result)
			case err = <-errChan:
				break RESULT_LOOP
			}
		}
		assert.NoError(t, err)
		n := 0
		if tc.found {
			n = 1
		}
		assert.Len(t, results, n)
		if tc.found {
			assert.Equal(t, results[0].Height, txResult.Height)
			assert.Equal(t, results[0].Index, txResult.Index)
		}
	}
}

func TestTxSearchMultipleTxs(t *testing.T) {
	// indexed first, but bigger height (to test the order of transactions)
	txResult := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "1", Index: true}}},
	})

	txResult.Tx = types.Tx("Bob's account")
	txResult.Height = 2
	txResult.Index = 0

	// indexed second, but smaller height (to test the order of transactions)
	txResult2 := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "2", Index: true}}},
	})
	txResult2.Tx = types.Tx("Alice's account")
	txResult2.Height = 1
	txResult2.Index = 0

	// indexed third (to test the order of transactions)
	txResult3 := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "3", Index: true}}},
	})
	txResult3.Tx = types.Tx("Jack's account")
	txResult3.Height = 1
	txResult3.Index = 1

	// indexed fourth (to test we don't include txs with similar events)
	// https://github.com/tendermint/tendermint/issues/2908
	txResult4 := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number.id", Value: "1", Index: true}}},
	})
	txResult4.Tx = types.Tx("Mike's account")
	txResult4.Height = 2
	txResult4.Index = 1

	blockStoreDB := db.NewPrefixDB(db.NewMemDB(), []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 2,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult2.Result,
			&txResult3.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)
	err = stateStore.SaveFinalizeBlockResponse(2, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult.Result,
			&txResult4.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	indexer := NewTxIndex(db.NewMemDB(), blockStore, stateStore, 0)

	batch := txindex.NewBatch(2)
	batch.Ops[0] = txResult2
	batch.Ops[1] = txResult3
	err = indexer.AddBatch(batch)
	require.NoError(t, err)

	batch = txindex.NewBatch(2)
	batch.Ops[0] = txResult
	batch.Ops[1] = txResult4
	err = indexer.AddBatch(batch)
	require.NoError(t, err)

	ctx := context.Background()

	resultChan, errChan := indexer.Search(ctx, query.MustCompile(`tx.height >= 1`), 1000)
	results := make([]abci.TxResult, 0)

RESULT_LOOP:
	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				break RESULT_LOOP
			}
			results = append(results, result)
		case err = <-errChan:
			break RESULT_LOOP
		}
	}
	assert.NoError(t, err)

	assert.Equal(t, results[0].Height, txResult2.Height)
	assert.Equal(t, results[0].Index, txResult2.Index)
	assert.Equal(t, results[1].Height, txResult3.Height)
	assert.Equal(t, results[1].Index, txResult3.Index)
	assert.Equal(t, results[2].Height, txResult.Height)
	assert.Equal(t, results[2].Index, txResult.Index)
	assert.Equal(t, results[3].Height, txResult4.Height)
}

func txResultWithEvents(events []abci.Event) *abci.TxResult {
	tx := types.Tx("HELLO WORLD")
	return &abci.TxResult{
		Height: 1,
		Index:  0,
		Tx:     tx,
		Result: abci.ExecTxResult{
			Data:   []byte{0},
			Code:   abci.CodeTypeOK,
			Log:    "",
			Events: events,
		},
	}
}

func benchmarkTxIndex(txsCount int64, b *testing.B) {
	dir, err := os.MkdirTemp("", "tx_index_db")
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	store, err := db.NewDB("tx_index", "goleveldb", dir)
	require.NoError(b, err)

	blockStoreDB := db.NewPrefixDB(store, []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 1,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(db.NewPrefixDB(store, []byte("state_store")), sm.StoreOptions{})

	indexer := NewTxIndex(store, blockStore, stateStore, 0)

	batch := txindex.NewBatch(txsCount)
	txIndex := uint32(0)
	txResults := make([]*abci.ExecTxResult, txsCount)
	for i := int64(0); i < txsCount; i++ {
		tx := cmtrand.Bytes(250)
		txResult := &abci.TxResult{
			Height: 1,
			Index:  txIndex,
			Tx:     tx,
			Result: abci.ExecTxResult{
				Data:   []byte{0},
				Code:   abci.CodeTypeOK,
				Log:    "",
				Events: []abci.Event{},
			},
		}
		txResults[i] = &txResult.Result
		if err := batch.Add(txResult); err != nil {
			b.Fatal(err)
		}
		txIndex++
	}

	b.ResetTimer()

	err = stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events:                []abci.Event{},
		TxResults:             txResults,
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	if err != nil {
		b.Fatal(err)
	}

	for n := 0; n < b.N; n++ {
		err = indexer.AddBatch(batch)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func TestBigInt(t *testing.T) {
	bigInt := "10000000000000000000"
	bigIntPlus1 := "10000000000000000001"
	bigFloat := bigInt + ".76"

	txResult := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: bigInt, Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: bigIntPlus1, Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "owner", Value: "/Ivan/", Index: true}}},
		{Type: "", Attributes: []abci.EventAttribute{{Key: "not_allowed", Value: "Vlad", Index: true}}},
	})
	hash := types.Tx(txResult.Tx).Hash()

	blockStoreDB := db.NewPrefixDB(db.NewMemDB(), []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 1,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	indexer := NewTxIndex(db.NewMemDB(), blockStore, stateStore, 0)

	batch := txindex.NewBatch(1)
	batch.Ops[0] = txResult
	err = indexer.AddBatch(batch)
	require.NoError(t, err)

	txResult2 := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: bigFloat, Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: bigFloat, Index: true}, {Key: "amount", Value: "5", Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: bigInt, Index: true}, {Key: "amount", Value: "3", Index: true}}}})

	txResult2.Tx = types.Tx("NEW TX")
	txResult2.Height = 2
	txResult2.Index = 0

	hash2 := types.Tx(txResult2.Tx).Hash()

	batch2 := txindex.NewBatch(1)
	batch2.Ops[0] = txResult2
	err = indexer.AddBatch(batch2)
	require.NoError(t, err)
	testCases := []struct {
		q             string
		txRes         *abci.TxResult
		resultsLength int
	}{
		//	search by hash
		{fmt.Sprintf("tx.hash = '%X'", hash), txResult, 1},
		// search by hash (lower)
		{fmt.Sprintf("tx.hash = '%x'", hash), txResult, 1},
		{fmt.Sprintf("tx.hash = '%x'", hash2), txResult2, 1},
		{"account.number = " + bigInt, nil, 1},
		{"account.number = " + bigIntPlus1 + " AND tx.height > 0", nil, 1},
		{"account.number = " + bigFloat + " AND tx.height > 0", nil, 0},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.q, func(t *testing.T) {
			resultChan, errChan := indexer.Search(ctx, query.MustCompile(tc.q), 1000)
			results := make([]abci.TxResult, 0)

			var err error
		RESULT_LOOP:
			for {
				select {
				case result, ok := <-resultChan:
					if !ok {
						break RESULT_LOOP
					}
					results = append(results, result)
				case err = <-errChan:
					break RESULT_LOOP
				}
			}
			assert.NoError(t, err)
			assert.Len(t, results, tc.resultsLength)
			if tc.resultsLength > 0 && tc.txRes != nil {
				assert.Equal(t, results[0].Height, tc.txRes.Height)
				assert.Equal(t, results[0].Index, tc.txRes.Index)
			}
		})
	}
}

func TestTxIndexPruning(t *testing.T) {
	txResult := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "1", Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "owner", Value: "/Ivan/", Index: true}}},
		{Type: "", Attributes: []abci.EventAttribute{{Key: "not_allowed", Value: "Vlad", Index: true}}},
	})
	txResult.Tx = types.Tx("tx1")
	hash := types.Tx(txResult.Tx).Hash()
	txResult.Height = 1

	txResult2 := txResultWithEvents([]abci.Event{
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "number", Value: "2", Index: true}}},
		{Type: "account", Attributes: []abci.EventAttribute{{Key: "owner", Value: "/Ivan2/", Index: true}}},
		{Type: "", Attributes: []abci.EventAttribute{{Key: "not_allowed", Value: "Vlad2", Index: true}}},
	})
	txResult2.Tx = types.Tx("tx2")
	hash2 := types.Tx(txResult2.Tx).Hash()
	txResult2.Height = 2

	blockStoreDB := db.NewPrefixDB(db.NewMemDB(), []byte("block_store"))
	bstore.SaveBlockStoreState(&cmtstore.BlockStoreState{
		Base:   1,
		Height: 101,
	}, blockStoreDB)
	blockStore := bstore.NewBlockStore(blockStoreDB)
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	err = stateStore.SaveFinalizeBlockResponse(2, &abci.ResponseFinalizeBlock{
		Events: []abci.Event{},
		TxResults: []*abci.ExecTxResult{
			&txResult2.Result,
		},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	indexer := NewTxIndex(db.NewMemDB(), blockStore, stateStore, 100)

	batch := txindex.NewBatch(1)
	batch.Ops[0] = txResult
	err = indexer.AddBatch(batch)
	require.NoError(t, err)

	batch2 := txindex.NewBatch(1)
	batch2.Ops[0] = txResult2
	err = indexer.AddBatch(batch2)
	require.NoError(t, err)

	// before pruning
	testCases := []struct {
		q                 string
		successAfterPrune bool
	}{
		// search by hash
		{fmt.Sprintf("tx.hash = '%X'", hash), false},
		// search by hash (lower)
		{fmt.Sprintf("tx.hash = '%x'", hash), false},
		// search by height
		{"tx.height = '1'", false},
		// search by exact match (one key)
		{"account.number = 1", false},
		{"account.owner = '/Ivan/'", false},

		{"tx.height >= 1", true},
		{fmt.Sprintf("tx.hash = '%X'", hash2), true},
		{"account.number = 2", true},
	}

	ctx := context.Background()
	// prune index
	err = indexer.Prune(101)
	require.NoError(t, err)
	// after pruning
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.q, func(t *testing.T) {
			resultChan, errChan := indexer.Search(ctx, query.MustCompile(tc.q), 1000)
			results := make([]abci.TxResult, 0)

			var err error
		RESULT_LOOP:
			for {
				select {
				case result, ok := <-resultChan:
					if !ok {
						break RESULT_LOOP
					}
					results = append(results, result)
				case err = <-errChan:
					break RESULT_LOOP
				}
			}
			assert.NoError(t, err)

			if tc.successAfterPrune {
				assert.Len(t, results, 1)
			} else {
				assert.Len(t, results, 0)
			}
		})
	}
}

func BenchmarkTxIndex1(b *testing.B)     { benchmarkTxIndex(1, b) }
func BenchmarkTxIndex500(b *testing.B)   { benchmarkTxIndex(500, b) }
func BenchmarkTxIndex1000(b *testing.B)  { benchmarkTxIndex(1000, b) }
func BenchmarkTxIndex2000(b *testing.B)  { benchmarkTxIndex(2000, b) }
func BenchmarkTxIndex10000(b *testing.B) { benchmarkTxIndex(10000, b) }
