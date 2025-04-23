package kv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	db "github.com/cometbft/cometbft-db"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	prototypes "github.com/cometbft/cometbft/proto/tendermint/types"
	blockidxkv "github.com/cometbft/cometbft/state/indexer/block/kv"
	"github.com/cometbft/cometbft/types"

	sm "github.com/cometbft/cometbft/state"
)

func TestBlockIndexer(t *testing.T) {
	store := db.NewPrefixDB(db.NewMemDB(), []byte("block_events"))
	events1 := []abci.Event{
		{
			Type: "begin_event",
			Attributes: []abci.EventAttribute{
				{
					Key:   "proposer",
					Value: "FCAA001",
					Index: true,
				},
			},
		},
		{
			Type: "end_event",
			Attributes: []abci.EventAttribute{
				{
					Key:   "foo",
					Value: "100",
					Index: true,
				},
			},
		},
	}

	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events:                events1,
		TxResults:             []*abci.ExecTxResult{},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)
	indexer := blockidxkv.New(store, stateStore, 0)

	require.NoError(t, indexer.Index(types.EventDataNewBlockEvents{
		Height: 1,
		Events: events1,
	}))

	for i := 2; i < 12; i++ {
		var index bool
		if i%2 == 0 {
			index = true
		}

		events := []abci.Event{
			{
				Type: "begin_event",
				Attributes: []abci.EventAttribute{
					{
						Key:   "proposer",
						Value: "FCAA001",
						Index: true,
					},
				},
			},
			{
				Type: "end_event",
				Attributes: []abci.EventAttribute{
					{
						Key:   "foo",
						Value: fmt.Sprintf("%d", i),
						Index: index,
					},
				},
			},
		}

		err := stateStore.SaveFinalizeBlockResponse(int64(i), &abci.ResponseFinalizeBlock{
			Events:                events,
			TxResults:             []*abci.ExecTxResult{},
			ValidatorUpdates:      []abci.ValidatorUpdate{},
			ConsensusParamUpdates: &prototypes.ConsensusParams{},
			AppHash:               []byte("app_hash"),
		})
		require.NoError(t, err)
		require.NoError(t, indexer.Index(types.EventDataNewBlockEvents{
			Height: int64(i),
			Events: events,
		}))
	}

	testCases := map[string]struct {
		q             *query.Query
		results       []int64
		expectedError bool
	}{
		"block.height = 100": {
			q:             query.MustCompile(`block.height = 100`),
			results:       []int64{},
			expectedError: false,
		},
		"block.height = 5": {
			q:             query.MustCompile(`block.height = 5`),
			results:       []int64{5},
			expectedError: false,
		},
		"begin_event.key1 = 'value1'": {
			q:             query.MustCompile(`begin_event.key1 = 'value1'`),
			results:       []int64{},
			expectedError: false,
		},
		"begin_event.proposer = 'FCAA001'": {
			q:             query.MustCompile(`begin_event.proposer = 'FCAA001'`),
			results:       []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			expectedError: false,
		},
		"end_event.foo = 100": {
			q:             query.MustCompile(`end_event.foo = 100`),
			results:       []int64{1},
			expectedError: false,
		},
		"block.height > 2 AND end_event.foo <= 8": {
			q:             query.MustCompile(`block.height > 2 AND end_event.foo <= 8`),
			results:       []int64{},
			expectedError: true,
		},
		"end_event.foo > 100": {
			q:             query.MustCompile("end_event.foo > 100"),
			results:       []int64{},
			expectedError: true,
		},
		"begin_event.proposer CONTAINS 'FFFFFFF'": {
			q:             query.MustCompile(`begin_event.proposer CONTAINS 'FFFFFFF'`),
			results:       []int64{},
			expectedError: true,
		},
		"end_event.foo CONTAINS '1'": {
			q:             query.MustCompile("end_event.foo CONTAINS '1'"),
			results:       []int64{},
			expectedError: true,
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			resultChan, errChan := indexer.Search(context.Background(), tc.q, 11, 1000)
			results := make([]int64, 0)

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
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.results, results)
			}
		})
	}
}

func TestBlockIndexerMulti(t *testing.T) {
	store := db.NewPrefixDB(db.NewMemDB(), []byte("block_events"))
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	indexer := blockidxkv.New(store, stateStore, 0)

	events1 := []abci.Event{
		{},
		{
			Type: "end_event",
			Attributes: []abci.EventAttribute{
				{
					Key:   "foo",
					Value: "100",
					Index: true,
				},
				{
					Key:   "bar",
					Value: "200",
					Index: true,
				},
			},
		},
		{
			Type: "end_event",
			Attributes: []abci.EventAttribute{
				{
					Key:   "foo",
					Value: "300",
					Index: true,
				},
				{
					Key:   "bar",
					Value: "500",
					Index: true,
				},
			},
		},
	}

	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events:                events1,
		TxResults:             []*abci.ExecTxResult{},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	require.NoError(t, indexer.Index(types.EventDataNewBlockEvents{
		Height: 1,
		Events: events1,
	}))

	events2 := []abci.Event{
		{},
		{
			Type: "end_event",
			Attributes: []abci.EventAttribute{
				{
					Key:   "foo",
					Value: "100",
					Index: true,
				},
				{
					Key:   "bar",
					Value: "200",
					Index: true,
				},
			},
		},
		{
			Type: "end_event",
			Attributes: []abci.EventAttribute{
				{
					Key:   "foo",
					Value: "300",
					Index: true,
				},
				{
					Key:   "bar",
					Value: "400",
					Index: true,
				},
			},
		},
	}

	err = stateStore.SaveFinalizeBlockResponse(2, &abci.ResponseFinalizeBlock{
		Events:                events2,
		TxResults:             []*abci.ExecTxResult{},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)
	require.NoError(t, indexer.Index(types.EventDataNewBlockEvents{
		Height: 2,
		Events: events2,
	}))

	testCases := map[string]struct {
		q             *query.Query
		results       []int64
		expectedError bool
	}{

		"query return all events from a height - exact": {
			q:             query.MustCompile("block.height = 1"),
			results:       []int64{1},
			expectedError: false,
		},
		"query return all events from a height - exact (deduplicate height)": {
			q:             query.MustCompile("block.height = 1 AND block.height = 2"),
			results:       []int64{1},
			expectedError: true,
		},
		"query return all events from a height - range": {
			q:             query.MustCompile("block.height < 2 AND block.height > 0 AND block.height > 0"),
			results:       []int64{1},
			expectedError: true,
		},
		"query return all events from a height - range 2": {
			q:             query.MustCompile("block.height < 3 AND block.height < 2 AND block.height > 0 AND block.height > 0"),
			results:       []int64{1},
			expectedError: true,
		},
		"query return all events from a height - range 3": {
			q:             query.MustCompile("block.height < 1 AND block.height > 1"),
			results:       []int64{},
			expectedError: false,
		},
		"query matches fields from same event": {
			q:             query.MustCompile("end_event.bar < 300 AND end_event.foo = 100 AND block.height > 0 AND block.height <= 2"),
			results:       []int64{},
			expectedError: true,
		},
		"query matches fields from multiple events": {
			q:             query.MustCompile("end_event.foo = 100 AND end_event.bar = 400 AND block.height = 2"),
			results:       []int64{2},
			expectedError: false,
		},
		"query matches fields from multiple events 2": {
			q:             query.MustCompile("end_event.foo = 100 AND end_event.bar > 200 AND block.height > 0 AND block.height < 3"),
			results:       []int64{},
			expectedError: true,
		},
		"query matches fields from multiple events allowed": {
			q:             query.MustCompile("end_event.foo = 100 AND end_event.bar = 400"),
			results:       []int64{2},
			expectedError: false,
		},
		"query using CONTAINS matches fields from all events whose attribute is within range": {
			q:             query.MustCompile("block.height  = 2 AND end_event.foo CONTAINS '30'"),
			results:       []int64{2},
			expectedError: true,
		},
		"query with non-existent field": {
			q:             query.MustCompile("end_event.baz = 100"),
			results:       []int64{},
			expectedError: false,
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			resultChan, errChan := indexer.Search(context.Background(), tc.q, 2, 1000)
			results := make([]int64, 0)

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
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.results, results)
			}
		})
	}
}

func TestBigInt(t *testing.T) {
	bigInt := "10000000000000000000"
	bigFloat := bigInt + ".76"
	bigFloatLower := bigInt + ".1"
	store := db.NewPrefixDB(db.NewMemDB(), []byte("block_events"))
	stateStore := sm.NewStore(db.NewPrefixDB(db.NewMemDB(), []byte("state_store")), sm.StoreOptions{})
	indexer := blockidxkv.New(store, stateStore, 0)

	events1 := []abci.Event{
		{},
		{
			Type: "end_event",
			Attributes: []abci.EventAttribute{
				{
					Key:   "foo",
					Value: "100",
					Index: true,
				},
				{
					Key:   "bar",
					Value: bigFloat,
					Index: true,
				},
				{
					Key:   "bar_lower",
					Value: bigFloatLower,
					Index: true,
				},
			},
		},
		{
			Type: "end_event",
			Attributes: []abci.EventAttribute{
				{
					Key:   "foo",
					Value: bigInt,
					Index: true,
				},
				{
					Key:   "bar",
					Value: "500",
					Index: true,
				},
				{
					Key:   "bla",
					Value: "500.5",
					Index: true,
				},
			},
		},
	}

	err := stateStore.SaveFinalizeBlockResponse(1, &abci.ResponseFinalizeBlock{
		Events:                events1,
		TxResults:             []*abci.ExecTxResult{},
		ValidatorUpdates:      []abci.ValidatorUpdate{},
		ConsensusParamUpdates: &prototypes.ConsensusParams{},
		AppHash:               []byte("app_hash"),
	})
	require.NoError(t, err)

	require.NoError(t, indexer.Index(types.EventDataNewBlockEvents{
		Height: 1,
		Events: events1,
	},
	))

	testCases := map[string]struct {
		q       *query.Query
		results []int64
	}{

		"query return all events from a height - exact": {
			q:       query.MustCompile("block.height = 1"),
			results: []int64{1},
		},
		"query matches fields with big int and height - match": {
			q:       query.MustCompile("end_event.foo = " + bigInt + " AND end_event.bar = 500 AND block.height = 1"),
			results: []int64{1},
		},
		"query matches big int in range": {
			q:       query.MustCompile("end_event.foo = " + bigInt),
			results: []int64{1},
		},
	}
	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			resultsChan, errChan := indexer.Search(context.Background(), tc.q, 1, 1000)
			results := make([]int64, 0)
			var err error
		RESULT_LOOP:
			for {
				select {
				case result, ok := <-resultsChan:
					if !ok {
						break RESULT_LOOP
					}
					results = append(results, result)
				case err = <-errChan:
					break RESULT_LOOP
				}
			}
			require.NoError(t, err)
			require.Equal(t, tc.results, results)
		})
	}
}

// func TestTxIndexPruning(t *testing.T) {
// 	indexer := blockidxkv.New(db.NewMemDB(), 100)

// 	blockEvents := types.EventDataNewBlockEvents{
// 		Height: 1,
// 		Events: []abci.Event{
// 			{},
// 			{
// 				Type: "account",
// 				Attributes: []abci.EventAttribute{
// 					{
// 						Key:   "number",
// 						Value: "1",
// 						Index: true,
// 					},
// 					{
// 						Key:   "owner",
// 						Value: "/Ivan/",
// 						Index: true,
// 					},
// 				},
// 			},
// 			{
// 				Type: "",
// 				Attributes: []abci.EventAttribute{
// 					{
// 						Key:   "not_allowed",
// 						Value: "Vlad",
// 						Index: true,
// 					},
// 				},
// 			},
// 		},
// 	}

// 	err := indexer.Index(blockEvents)
// 	require.NoError(t, err)

// 	// before pruning
// 	testCases := []struct {
// 		q                 string
// 		successAfterPrune bool
// 	}{
// 		//search by height
// 		{"block.height = 1", false},
// 		// search by exact match (one key)
// 		{"account.number = 1", false},
// 		{"account.owner = '/Ivan/'", false},
// 		// search by range
// 		{"account.number >= 1 AND account.number <= 5", false},
// 		// search by range (lower bound)
// 		{"account.number >= 1", false},
// 		// search by range (upper bound)
// 		{"account.number <= 5", false},
// 		{"account.number <= 1", false},
// 		// search using CONTAINS
// 		{"account.owner CONTAINS 'an'", false},
// 		// search using EXISTS
// 		{"account.number EXISTS", false},
// 	}

// 	ctx := context.Background()

// 	for _, tc := range testCases {
// 		tc := tc
// 		t.Run(tc.q, func(t *testing.T) {
// 			results, err := indexer.Search(ctx, query.MustCompile(tc.q))
// 			require.NoError(t, err)

// 			require.Len(t, results, 1)
// 			for _, h := range results {
// 				require.Equal(t, int64(1), h)
// 			}
// 		})
// 	}

// 	// prune index
// 	indexer.Prune(101)

// 	// after pruning
// 	for _, tc := range testCases {
// 		tc := tc
// 		t.Run(tc.q, func(t *testing.T) {
// 			results, err := indexer.Search(ctx, query.MustCompile(tc.q))
// 			require.NoError(t, err)

// 			if tc.successAfterPrune {
// 				require.Len(t, results, 1)
// 				for _, h := range results {
// 					require.Equal(t, int64(1), h)
// 				}
// 			} else {
// 				require.Len(t, results, 0)
// 			}
// 		})
// 	}
// }
