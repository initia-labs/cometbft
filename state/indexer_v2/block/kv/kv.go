package kv

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"golang.org/x/sync/errgroup"

	dbm "github.com/cometbft/cometbft-db"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	"github.com/cometbft/cometbft/libs/pubsub/query/syntax"
	indexer "github.com/cometbft/cometbft/state/indexer_v2"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"

	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/state/filtermaps"
)

var _ indexer.BlockIndexer = (*BlockIndexer)(nil)

// BlockIndexer implements a block indexer, indexing FinalizeBlock
// events with an underlying KV store. Block events are indexed by their height,
// such that matching search criteria returns the respective block height(s).
type BlockIndexer struct {
	store dbm.DB

	blockStore *store.BlockStore
	stateStore sm.Store

	log log.Logger

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

func New(store dbm.DB, blockStore *store.BlockStore, stateStore sm.Store, retainHeight int64) *BlockIndexer {
	fm := filtermaps.NewFilterMaps(dbm.NewPrefixDB(store, []byte("filtermap")), blockStore, stateStore, filtermaps.DefaultParams, filtermaps.Config{
		History:     uint64(retainHeight),
		Disabled:    false,
		IsTxIndexer: false,
	})

	return &BlockIndexer{
		store:                 store,
		blockStore:            blockStore,
		stateStore:            stateStore,
		log:                   log.NewNopLogger(),
		retainHeight:          retainHeight,
		filtermap:             fm,
		unlockBlockProcessing: sync.Once{},
	}
}

func (idx *BlockIndexer) Start() {
	idx.filtermap.SetBlockProcessing(true)
	idx.filtermap.Start()
}

func (idx *BlockIndexer) SetLogger(l log.Logger) {
	idx.log = l
	idx.filtermap.SetLogger(l)
}

func (idx *BlockIndexer) Has(height int64) (bool, error) {
	_, err := idx.stateStore.LoadFinalizeBlockResponse(height)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Index indexes FinalizeBlock events for a given block by its height.
// The following is indexed:
//
// block bloom: encode(bb | height) => block bloom
// section bloom: encode(sb | sectionIndex) => section bloom
func (idx *BlockIndexer) Index(bh types.EventDataNewBlockEvents) error {
	idx.unlockBlockProcessing.Do(func() {
		idx.filtermap.SetBlockProcessing(false)
	})

	idx.filtermap.SetTarget(uint64(bh.Height - 1))
	return nil
}

// Search performs a query for block heights that match a given FinalizeBlock
// event search criteria. The given query can match against zero,
// one or more block heights. In the case of height queries, i.e. block.height=H,
// if the height is indexed, that height alone will be returned. An error and
// nil slice is returned. Otherwise, a non-nil slice and nil error is returned.
func (idx *BlockIndexer) Search(ctx context.Context, q *query.Query) (chan int64, chan error) {
	resultChan := make(chan int64)
	errChan := make(chan error)

	go func() {
		defer func() {
			close(resultChan)
			close(errChan)
		}()

		errChan <- idx.search(ctx, q, resultChan)
	}()
	return resultChan, errChan
}

func (idx *BlockIndexer) search(ctx context.Context, q *query.Query, resultCh chan int64) error {
	select {
	case <-ctx.Done():
		return nil

	default:
	}

	conditions := q.Syntax()

	// If we are not matching events and block.height occurs more than once, the later value will
	// overwrite the first one.
	conditions, heightInfo, err := dedupHeight(conditions)
	if err != nil {
		return err
	}

	// Extract ranges. If both upper and lower bounds exist, it's better to get
	// them in order as to not iterate over kvs that are not within range.
	_, _, heightRange, err := indexer.LookForRangesWithHeight(conditions)
	if err != nil {
		return err
	}
	heightInfo.heightRange = heightRange

	// If we have additional constraints and want to query per event
	// attributes, we cannot simply return all blocks for a height.
	// But we remember the height we want to find and forward it to
	// match(). If we only have the height constraint
	// in the query (the second part of the ||), we don't need to query
	// per event conditions and return all events within the height range.
	if heightInfo.onlyHeightEq {
		ok, err := idx.Has(heightInfo.height)
		if err != nil {
			return err
		}

		if ok {
			resultCh <- heightInfo.height
		}
		return nil
	}

	filters, err := filtersFromConditions(conditions)
	if err != nil {
		return err
	}

	begin := int64(1)
	end := idx.blockStore.Height()

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

	begin = max(begin, idx.blockStore.Base())
	end = min(end, int64(idx.filtermap.GetLastIndexedBlock()))

	// if the begin is greater than the end, return nil
	if begin > end {
		return nil
	}

	backend := idx.filtermap.NewMatcherBackend()

	filtermapResultCh := make(chan *filtermaps.TxEvent)
	g, innerCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(filtermapResultCh)
		return filtermaps.GetPotentialMatches(innerCtx, idx.log, backend, uint64(begin-1), uint64(end-1), filters, filtermapResultCh)
	})

	blockCache := make(map[int64]*abci.ResponseFinalizeBlock)
	g.Go(func() error {
		lastEvent := &filtermaps.TxEvent{}
		for result := range filtermapResultCh {
			if result == nil || (result.BlockNumber == lastEvent.BlockNumber) {
				continue
			}

			blockResponse, ok := blockCache[result.BlockNumber]
			if !ok {
				blockResponse, err = idx.stateStore.LoadFinalizeBlockResponse(result.BlockNumber)
				if err != nil {
					return err
				}
				blockCache[result.BlockNumber] = blockResponse
			}

			blockNumber := idx.checkMatch(result, filters, blockResponse.Events)
			lastEvent = result
			if blockNumber >= 0 {
				resultCh <- blockNumber
			}
		}
		return nil
	})
	return g.Wait()
}

func (idx *BlockIndexer) checkMatch(txEvent *filtermaps.TxEvent, filters []string, events []abci.Event) int64 {
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
		return txEvent.BlockNumber
	}
	return -1
}

func filtersFromConditions(conditions []syntax.Condition) ([]string, error) {
	var filters []string
	for _, c := range conditions {
		if c.Tag == types.BlockHeightKey {
			continue
		} else if c.Tag == types.TxHeightKey {
			return nil, fmt.Errorf("tx height is not allowed in the query")
		}

		if c.Op == syntax.TEq {
			filters = append(filters, fmt.Sprintf("%s=%s", c.Tag, c.Arg.Value()))
		} else {
			return nil, fmt.Errorf("unsupported operation: %s", c.Op)
		}
	}
	return filters, nil
}
