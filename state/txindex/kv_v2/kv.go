package kv

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"

	"golang.org/x/sync/errgroup"

	"github.com/cometbft/cometbft/libs/log"

	"github.com/cosmos/gogoproto/proto"

	dbm "github.com/cometbft/cometbft-db"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	"github.com/cometbft/cometbft/libs/pubsub/query/syntax"
	indexerv2 "github.com/cometbft/cometbft/state/indexer_v2"
	"github.com/cometbft/cometbft/state/txindex"
	"github.com/cometbft/cometbft/types"

	"github.com/cometbft/cometbft/state/filtermaps"

	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"

	"sync"
)

var _ txindex.TxIndexerV2 = (*TxIndex)(nil)

// TxIndex is the simplest possible indexer, backed by key-value storage (levelDB).
type TxIndex struct {
	store dbm.DB

	log log.Logger

	blockStore *store.BlockStore
	stateStore sm.Store

	// The minimum tx height offsets from the current block being committed,
	// such that all txs past this offset are pruned.
	//
	// If set to 0, the index will retain all tx index.
	// Else the index will retain txs and blocks with heights >= (current block height - RetainHeight)
	// except "tx.hash" and "tx.height" and "block.height" which are always retained.
	retainHeight int64

	filtermap *filtermaps.FilterMaps

	unlockBlockProcessing sync.Once
}

// NewTxIndex creates new KV indexer.
func NewTxIndex(store dbm.DB, blockStore *store.BlockStore, stateStore sm.Store, retainHeight int64) *TxIndex {
	fm := filtermaps.NewFilterMaps(dbm.NewPrefixDB(store, []byte("filtermap")), blockStore, stateStore, filtermaps.DefaultParams, filtermaps.Config{
		History:     uint64(retainHeight),
		Disabled:    false,
		IsTxIndexer: true,
	})

	return &TxIndex{
		store:                 store,
		log:                   log.NewNopLogger(),
		blockStore:            blockStore,
		stateStore:            stateStore,
		retainHeight:          retainHeight,
		filtermap:             fm,
		unlockBlockProcessing: sync.Once{},
	}
}

func (txi *TxIndex) Start() {
	txi.filtermap.SetBlockProcessing(true)
	txi.filtermap.Start()
}

func (txi *TxIndex) SetLogger(l log.Logger) {
	txi.log = l
	txi.filtermap.SetLogger(l)
}

// Get gets transaction from the TxIndex storage and returns it or nil if the
// transaction is not found.
func (txi *TxIndex) Get(hash []byte) (*abci.TxResult, error) {
	if len(hash) == 0 {
		return nil, txindex.ErrorEmptyHash
	}

	rawBytes, err := txi.store.Get(hash)
	if err != nil {
		panic(err)
	}
	if rawBytes == nil {
		return nil, nil
	}

	txResult := new(abci.TxResult)
	err = proto.Unmarshal(rawBytes, txResult)
	if err != nil {
		return nil, fmt.Errorf("error reading TxResult: %v", err)
	}

	return txResult, nil
}

func (txi *TxIndex) AddBatch(b *txindex.Batch, height int64) error {
	storeBatch := txi.store.NewBatch()
	defer storeBatch.Close()

	for _, result := range b.Ops {
		tmpResult := *result
		hash := types.Tx(result.Tx).Hash()

		// index by height (always)
		err := storeBatch.Set(keyForHeight(result), hash)
		if err != nil {
			return err
		}

		tmpResult.Result = abci.ExecTxResult{}
		tmpResult.Tx = nil

		rawBytes, err := proto.Marshal(&tmpResult)
		if err != nil {
			return err
		}
		// index by hash (always)
		err = storeBatch.Set(hash, rawBytes)
		if err != nil {
			return err
		}
	}

	err := storeBatch.WriteSync()
	if err != nil {
		return err
	}

	txi.unlockBlockProcessing.Do(func() {
		txi.filtermap.SetBlockProcessing(false)
	})

	txi.filtermap.SetTarget(uint64(height - 1))
	return nil
}

// Search performs a search using the given query.
//
// It breaks the query into conditions (like "tx.height > 5"). For each
// condition, it queries the DB index. One special use cases here: (1) if
// "tx.hash" is found, it returns tx result for it (2) for range queries it is
// better for the client to provide both lower and upper bounds, so we are not
// performing a full scan. Results from querying indexes are then intersected
// and returned to the caller, in no particular order.
//
// Search will exit early and return any result fetched so far,
// when a message is received on the context chan.
func (txi *TxIndex) Search(ctx context.Context, q *query.Query) (chan abci.TxResult, chan error) {
	resultCh := make(chan abci.TxResult)
	errCh := make(chan error)

	go func() {
		defer func() {
			close(resultCh)
			close(errCh)
		}()

		errCh <- txi.search(ctx, q, resultCh)
	}()
	return resultCh, errCh
}

func (txi *TxIndex) search(ctx context.Context, q *query.Query, resultCh chan abci.TxResult) error {
	select {
	case <-ctx.Done():
		return nil

	default:
	}

	// get a list of conditions (like "tx.height > 5")
	conditions := q.Syntax()
	// if there is a hash condition, return the result immediately
	hash, ok, err := lookForHash(conditions)
	if err != nil {
		return fmt.Errorf("error during searching for a hash in the query: %w", err)
	} else if ok {
		res, err := txi.Get(hash)
		switch {
		case err != nil:
			return fmt.Errorf("error while retrieving the result: %w", err)
		case res == nil:
			return nil
		default:
			resultCh <- *res
			return nil
		}
	}

	// If we are not matching events and tx.height = 3 occurs more than once, the later value will
	// overwrite the first one.
	conditions, heightInfo, err := dedupHeight(conditions)
	if err != nil {
		return err
	}

	// extract ranges
	// if both upper and lower bounds exist, it's better to get them in order not
	// no iterate over kvs that are not within range.
	_, _, heightRange, err := indexerv2.LookForRangesWithHeight(conditions)
	if err != nil {
		return err
	}

	heightInfo.heightRange = heightRange

	// filtermap only supports equality operator, otherwise it will return an error
	filters, err := filtersFromConditions(conditions)
	if err != nil {
		return err
	}

	begin := int64(1)
	end := txi.blockStore.Height()

	if heightInfo.height != 0 {
		begin = heightInfo.height
		end = heightInfo.height
	} else if heightInfo.heightRange.Key != "" {
		if heightInfo.heightRange.LowerBound != nil {
			bigBegin, ok := heightInfo.heightRange.LowerBound.(*big.Float)
			if !ok {
				return fmt.Errorf("invalid height range lower bound: %v", heightInfo.heightRange.LowerBound)
			}
			begin, _ = bigBegin.Int64()
			if !heightInfo.heightRange.IncludeLowerBound {
				begin++
			}
		}
		if heightInfo.heightRange.UpperBound != nil {
			bigEnd, ok := heightInfo.heightRange.UpperBound.(*big.Float)
			if !ok {
				return fmt.Errorf("invalid height range upper bound: %v", heightInfo.heightRange.UpperBound)
			}
			rangeEnd, _ := bigEnd.Int64()
			if !heightInfo.heightRange.IncludeUpperBound {
				rangeEnd--
			}
			end = min(end, rangeEnd)
		}
	}

	begin = max(begin, txi.blockStore.Base())
	end = min(end, int64(txi.filtermap.GetLastIndexedBlock()))

	// if the begin is greater than the end, return nil
	if begin > end {
		return nil
	}

	backend := txi.filtermap.NewMatcherBackend()

	filtermapResultCh := make(chan *filtermaps.TxEvent)
	g, innerCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(filtermapResultCh)
		// filtermap uses block number starting from 0, so we need to subtract 1 from begin and end
		return filtermaps.GetPotentialMatches(innerCtx, txi.log, backend, uint64(begin-1), uint64(end-1), filters, filtermapResultCh)
	})

	blockCache := make(map[int64]*abci.ResponseFinalizeBlock)
	g.Go(func() error {
		lastEvent := &filtermaps.TxEvent{}
		for result := range filtermapResultCh {
			if result == nil || (result.BlockNumber == lastEvent.BlockNumber && result.TxIndex == lastEvent.TxIndex) {
				continue
			}

			blockResponse, ok := blockCache[result.BlockNumber]
			if !ok {
				blockResponse, err = txi.stateStore.LoadFinalizeBlockResponse(result.BlockNumber)
				if err != nil {
					return err
				}
				blockCache[result.BlockNumber] = blockResponse
			}

			txResult := txi.checkMatch(result, filters, blockResponse.TxResults[result.TxIndex].Events)
			lastEvent = result
			if txResult != nil {
				resultCh <- *txResult
			}
		}
		return nil
	})
	return g.Wait()
}

func (txi *TxIndex) checkMatch(txEvent *filtermaps.TxEvent, filters []string, events []abci.Event) *abci.TxResult {
	matchCount := 0
FILTERLOOP:
	for _, filter := range filters {
		for _, event := range events {
			for _, attr := range event.Attributes {
				eventString := filtermaps.EventString(event.Type, attr)
				if eventString == filter {
					matchCount++
					continue FILTERLOOP
				}
			}
		}
	}
	if matchCount == len(filters) {
		return &abci.TxResult{
			Height: txEvent.BlockNumber,
			Index:  uint32(txEvent.TxIndex),
		}
	}
	return nil
}

func lookForHash(conditions []syntax.Condition) (hash []byte, ok bool, err error) {
	for _, c := range conditions {
		if c.Tag == types.TxHashKey {
			decoded, err := hex.DecodeString(c.Arg.Value())
			return decoded, true, err
		}
	}
	return
}

func keyForHeight(result *abci.TxResult) []byte {
	return []byte(fmt.Sprintf("%s/%d/%d",
		types.TxHeightKey,
		result.Height,
		result.Index,
	))
}

func filtersFromConditions(conditions []syntax.Condition) ([]string, error) {
	var filters []string
	for _, c := range conditions {
		if c.Tag == types.TxHeightKey {
			continue
		} else if c.Tag == types.BlockHeightKey {
			return nil, fmt.Errorf("block height is not allowed in the query")
		}

		if c.Op == syntax.TEq {
			filters = append(filters, fmt.Sprintf("%s=%s", c.Tag, c.Arg.Value()))
		} else {
			return nil, fmt.Errorf("unsupported operation: %s", c.Op)
		}
	}
	return filters, nil
}

func (txi *TxIndex) Prune(curHeight int64) error {
	minHeight := curHeight - txi.retainHeight
	if minHeight <= 0 || minHeight >= curHeight {
		return nil
	}

	pruneBatch := txi.store.NewBatch()
	defer pruneBatch.Close()

	base := txi.blockStore.Base()

	iter, err := txi.store.Iterator(keyForHeight(&abci.TxResult{Height: base}), keyForHeight(&abci.TxResult{Height: minHeight + 1}))
	if err != nil {
		return err
	}
	defer iter.Close()

	for ; iter.Valid(); iter.Next() {
		if err := pruneBatch.Delete(iter.Key()); err != nil {
			return err
		}

		// tx hash
		if err := pruneBatch.Delete(iter.Value()); err != nil {
			return err
		}
	}
	return pruneBatch.WriteSync()
}
