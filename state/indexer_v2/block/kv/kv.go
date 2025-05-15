package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	dbm "github.com/cometbft/cometbft-db"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	"github.com/cometbft/cometbft/libs/pubsub/query/syntax"
	indexer "github.com/cometbft/cometbft/state/indexer_v2"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"

	"github.com/ethereum/go-ethereum/core/bloombits"
	gethcoretypes "github.com/ethereum/go-ethereum/core/types"

	sm "github.com/cometbft/cometbft/state"
)

var _ indexer.BlockIndexer = (*BlockerIndexer)(nil)

const (
	bloomSectionSize = int64(4096)

	sectionBloomKeyPrefix = "sb"
	blockBloomKeyPrefix   = "bb"
	baseKey               = "base"
	heightKey             = "h"
	sectionIndexKey       = "si"
	migrationKey          = "migration"

	// bloomServiceThreads is the number of goroutines used globally by an Ethereum
	// instance to service bloombits lookups for all running filters.
	bloomServiceThreads = 16

	// bloomFilterThreads is the number of goroutines used locally per filter to
	// multiplex requests onto the global servicing goroutines.
	bloomFilterThreads = 3

	// bloomRetrievalBatch is the maximum number of bloom bit retrievals to service
	// in a single batch.
	bloomRetrievalBatch = 16

	// bloomRetrievalWait is the maximum time to wait for enough bloom bit requests
	// to accumulate request an entire batch (avoiding hysteresis).
	bloomRetrievalWait = time.Duration(0)
)

// BlockerIndexer implements a block indexer, indexing FinalizeBlock
// events with an underlying KV store. Block events are indexed by their height,
// such that matching search criteria returns the respective block height(s).
type BlockerIndexer struct {
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

	// isMigrating is true if the indexer is migrating from the old indexer to the new one.
	isMigrating bool

	newBlockNotifier    chan struct{}
	sectionBloomRunning atomic.Bool
}

func New(store dbm.DB, blockStore *store.BlockStore, stateStore sm.Store, retainHeight int64) *BlockerIndexer {
	idx := &BlockerIndexer{
		store:        store,
		blockStore:   blockStore,
		stateStore:   stateStore,
		log:          log.NewNopLogger(),
		retainHeight: retainHeight,

		newBlockNotifier: make(chan struct{}),
	}
	idx.sectionBloomRunning.Store(false)

	go idx.startSectionBloomCreation()

	return idx
}

func (idx *BlockerIndexer) SetLogger(l log.Logger) {
	idx.log = l
}

// Has returns true if the given height has been indexed. An error is returned
// upon database query failure.
func (idx *BlockerIndexer) Has(height int64) (bool, error) {
	return idx.store.Has(bloomKeyForBlock(height))
}

// Index indexes FinalizeBlock events for a given block by its height.
// The following is indexed:
//
// block bloom: encode(bb | height) => block bloom
// section bloom: encode(sb | sectionIndex) => section bloom
func (idx *BlockerIndexer) Index(bh types.EventDataNewBlockEvents) error {
	batch := idx.store.NewBatch()
	defer batch.Close()

	// update block bloom
	blockBloom := bloomForBlock(bh.Events)
	err := batch.Set(bloomKeyForBlock(bh.Height), blockBloom[:])
	if err != nil {
		return err
	}

	base, err := idx.Base()
	if err != nil {
		return err
	} else if base == 0 || base > bh.Height {
		err = batch.Set([]byte(baseKey), int64ToBytes(bh.Height))
		if err != nil {
			return err
		}
	}

	// store last indexed height
	height, err := idx.Height()
	if err != nil {
		return err
	} else if height < bh.Height {
		err = batch.Set([]byte(heightKey), int64ToBytes(bh.Height))
		if err != nil {
			return err
		}
	}

	err = batch.WriteSync()
	if err != nil {
		return err
	}

	// if the section bloom is not running, start it and update the flag
	if idx.sectionBloomRunning.CompareAndSwap(false, true) {
		idx.newBlockNotifier <- struct{}{}
	}
	return nil
}

// startSectionBloomCreation creates a section bloom for the given height in a separate goroutine.
func (idx *BlockerIndexer) startSectionBloomCreation() {
	logger := idx.log.With("function", "SectionBloomCreation")

	creationFn := func() {
		// reset the flag when the function is done
		defer idx.sectionBloomRunning.Store(false)

		height, err := idx.Height()
		if err != nil {
			logger.Error("failed to get height", "err", err)
			return
		}

		dbSectionIndex, err := idx.SectionIndex()
		if err != nil {
			logger.Error("failed to get section index", "err", err)
			return
		}

		// skip if the section bloom is already up to date
		sectionIndex := latestReadySectionIndex(height)
		if dbSectionIndex >= sectionIndex {
			return
		}

		// start the bloom indexing and log the start
		logger.Debug("section bloom indexing started", "height", height)

		// create a new batch
		batch := idx.store.NewBatch()
		err = idx.createSectionBloom(dbSectionIndex+1, batch)
		if err != nil {
			logger.Error("failed to do bloom indexing", "err", err)
			return
		}

		// write the batch to the store
		if err := batch.WriteSync(); err != nil {
			logger.Error("failed to write sync", "err", err)
			return
		}

		// close the batch
		if err := batch.Close(); err != nil {
			logger.Error("failed to close batch", "err", err)
			return
		}

		// log the completion
		logger.Info("section bloom indexing finished", "height", height, "sectionIndex", sectionIndex)
	}

	for range idx.newBlockNotifier {
		creationFn()
	}
}

// createSectionBloom creates a section bloom for the given section index.
func (idx *BlockerIndexer) createSectionBloom(sectionIndex int64, batch dbm.Batch) error {
	gen, err := bloombits.NewGenerator(uint(bloomSectionSize))
	if err != nil {
		return err
	}

	for i := range bloomSectionSize {
		blockBloom, err := idx.store.Get(bloomKeyForBlock(sectionIndex*bloomSectionSize + i))
		if err != nil {
			return err
		} else if blockBloom == nil {
			blockBloom = make([]byte, gethcoretypes.BloomBitLength/8)
		}

		if err := gen.AddBloom(uint(i), gethcoretypes.Bloom(blockBloom)); err != nil {
			return err
		}
	}

	// write the bloom bits to the store
	for i := range gethcoretypes.BloomBitLength {
		bits, err := gen.Bitset(uint(i))
		if err != nil {
			return err
		}

		err = batch.Set(bloomKeyForSectionIndex(sectionIndex, int64(i)), bits)
		if err != nil {
			return err
		}
	}

	return batch.Set([]byte(sectionIndexKey), int64ToBytes(sectionIndex))
}

// Search performs a query for block heights that match a given FinalizeBlock
// event search criteria. The given query can match against zero,
// one or more block heights. In the case of height queries, i.e. block.height=H,
// if the height is indexed, that height alone will be returned. An error and
// nil slice is returned. Otherwise, a non-nil slice and nil error is returned.
func (idx *BlockerIndexer) Search(ctx context.Context, q *query.Query, maxCount int64) (chan int64, chan error) {
	resultChan := make(chan int64)
	errChan := make(chan error)

	go func() {
		defer func() {
			close(resultChan)
			close(errChan)
		}()

		errChan <- idx.search(ctx, q, maxCount, resultChan)
	}()
	return resultChan, errChan
}

func (idx *BlockerIndexer) search(ctx context.Context, q *query.Query, maxCount int64, resultChan chan int64) error {
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
			resultChan <- heightInfo.height
		}
		return nil
	} else if idx.isMigrating {
		return fmt.Errorf("indexer is migrating, only height search is supported")
	}

	filters, err := filtersFromConditions(conditions)
	if err != nil {
		return err
	}

	begin := int64(1)
	height, err := idx.Height()
	if err != nil {
		return err
	} else if height == 0 {
		return fmt.Errorf("no data exists")
	}
	end := height

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

	// for indexed events

	innerCountForIndexed := int64(0)

	idxBase, err := idx.Base()
	if err != nil {
		return err
	}
	begin = max(begin, idxBase, idx.blockStore.Base())

	// if the begin is greater than the end, return nil
	if begin > end {
		return nil
	}

	sectionIndex, err := idx.SectionIndex()
	if err != nil {
		return err
	} else if indexed := (sectionIndex + 1) * bloomSectionSize; indexed > begin {
		endForIndexed := min(end, indexed-1)
		matches := make(chan uint64, 64)

		matcher := bloombits.NewMatcher(uint64(bloomSectionSize), [][][]byte{filters})
		session, err := matcher.Start(ctx, uint64(begin), uint64(endForIndexed), matches)
		if err != nil {
			return err
		}

		bloomRequests := make(chan chan *bloombits.Retrieval)
		for range bloomServiceThreads {
			go func() {
				for {
					select {
					case <-ctx.Done():
						return

					case request := <-bloomRequests:
						task := <-request
						task.Bitsets = make([][]byte, len(task.Sections))
						for i, section := range task.Sections {
							sectionBitbloom, err := idx.store.Get(bloomKeyForSectionIndex(int64(section), int64(task.Bit)))
							if err != nil {
								task.Error = err
								break
							} else if sectionBitbloom == nil {
								// pruned section, return empty bitset
								task.Bitsets[i] = make([]byte, bloomSectionSize/8)
								continue
							}
							task.Bitsets[i] = sectionBitbloom
						}
						request <- task
					}
				}
			}()
		}

		for range bloomFilterThreads {
			go session.Multiplex(bloomRetrievalBatch, bloomRetrievalWait, bloomRequests)
		}

	MATCHES_LOOP:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case number, ok := <-matches:
				// Abort if all matches have been fulfilled
				if !ok {
					err = session.Error()
					break MATCHES_LOOP
				}
				match, err := idx.checkMatch(int64(number), filters)
				if err != nil {
					return err
				}
				if match {
					innerCountForIndexed++
					resultChan <- int64(number)
				}
			}
		}
		if err != nil {
			return err
		}

		begin = max(begin, endForIndexed+1)
	}

	const batchSize = 500
	innerCountForUnindexed := atomic.Int64{}

	g, innerCtx := errgroup.WithContext(ctx)
	diff := end - begin + 1
	if diff >= bloomSectionSize*2 {
		return fmt.Errorf("insufficient indexed data, reduce the query range")
	}
	batchNum := diff / batchSize
	if diff%batchSize != 0 {
		batchNum++
	}

	resultsArray := make([][]int64, batchNum)
	for i := int64(0); i < batchNum; i++ {
		// make local copy of i for goroutine
		batchIdx := i
		batchBegin := begin + i*batchSize
		batchEnd := min(batchBegin+batchSize-1, end)

		// fetch logs in parallel
		g.Go(func() error {
			for batchNumber := batchBegin; batchNumber <= batchEnd; batchNumber++ {
				select {
				case <-innerCtx.Done():
					return innerCtx.Err()
				default:
				}

				found, err := idx.checkMatch(batchNumber, filters)
				if err != nil {
					return err
				} else if found {
					innerCountForUnindexed.Add(1)
					if innerCountForUnindexed.Load()+innerCountForIndexed >= maxCount {
						return errors.New("too many results, reduce the query range")
					}
					resultsArray[batchIdx] = append(resultsArray[batchIdx], batchNumber)
				}
			}
			return nil
		})
	}

	// wait for all goroutines to finish
	err = g.Wait()
	if err != nil {
		return err
	}

	// send logs to channel in order
	for _, results := range resultsArray {
		for _, result := range results {
			select {
			case resultChan <- result:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

func (idx *BlockerIndexer) checkMatch(number int64, filters [][]byte) (bool, error) {
	response, err := idx.stateStore.LoadFinalizeBlockResponse(number)
	if err != nil || response == nil {
		return false, nil
	}

EVENTSCHECK_LOOP:
	for _, conditionFilter := range filters {
		for _, event := range response.Events {
			if len(event.Type) == 0 {
				continue
			}
			for _, attr := range event.Attributes {
				if len(attr.Key) == 0 {
					continue
				} else if attr.Index {
					filter := eventFilter(event.Type, attr.Key, attr.Value)
					if bytes.Equal(conditionFilter, filter) {
						continue EVENTSCHECK_LOOP
					}
				}
			}
		}

		// no match found
		return false, nil
	}
	return true, err
}

func (idx *BlockerIndexer) Prune(curHeight int64) error {
	minHeight := curHeight - idx.retainHeight
	if minHeight <= 0 || minHeight >= curHeight {
		return nil
	}

	pruneBatch := idx.store.NewBatch()
	defer pruneBatch.Close()

	base, err := idx.Base()
	if err != nil {
		return err
	}

	// end key is exclusive
	iter, err := idx.store.Iterator(bloomKeyForBlock(base), bloomKeyForBlock(minHeight+1))
	if err != nil {
		return err
	}
	defer iter.Close()

	for ; iter.Valid(); iter.Next() {
		if err := pruneBatch.Delete(iter.Key()); err != nil {
			return err
		}
	}

	iter2, err := idx.store.Iterator(bloomKeyForSectionIndex(base/bloomSectionSize, 0), bloomKeyForSectionIndex(minHeight/bloomSectionSize, bloomSectionSize))
	if err != nil {
		return err
	}
	defer iter2.Close()

	for ; iter2.Valid(); iter2.Next() {
		if err := pruneBatch.Delete(iter2.Key()); err != nil {
			return err
		}
	}

	err = pruneBatch.Set([]byte(baseKey), int64ToBytes(minHeight+1))
	if err != nil {
		return err
	}
	return pruneBatch.WriteSync()
}

func (idx *BlockerIndexer) Base() (int64, error) {
	base, err := idx.store.Get([]byte(baseKey))
	if err != nil {
		return 0, err
	} else if base == nil {
		return 0, nil
	}
	return int64FromBytes(base), nil
}

func (idx *BlockerIndexer) Height() (int64, error) {
	height, err := idx.store.Get([]byte(heightKey))
	if err != nil {
		return 0, err
	} else if height == nil {
		return 0, nil
	}
	return int64FromBytes(height), nil
}

func (idx *BlockerIndexer) SectionIndex() (int64, error) {
	sectionIndex, err := idx.store.Get([]byte(sectionIndexKey))
	if err != nil {
		return 0, err
	} else if sectionIndex == nil {
		return -1, nil
	}
	return int64FromBytes(sectionIndex), nil
}

// latestReadySectionIndex returns the section index for a given height, where all blocks in that section
// are guaranteed to be ready. The section index is calculated by dividing the height by the bloom section
// size (4096) and subtracting 1. This ensures we only return a section once all its blocks are available.
//
// For example, with a section size of 4096 blocks:
// - Section -1 contains heights [0, 4095]     - Ready when height >= 4096
// - Section 0 contains heights [4096, 8191]   - Ready when height >= 8192
// - Section 1 contains heights [8192, 12287]  - Ready when height >= 12288
//
// This approach prevents returning incomplete sections that are still being filled with blocks.
func latestReadySectionIndex(height int64) int64 {
	return height/bloomSectionSize - 1
}

func eventFilter(eventType string, attrKey string, attrValue string) []byte {
	return fmt.Appendf(nil, "%s.%s=%s", eventType, attrKey, attrValue)
}

func bloomForBlock(events []abci.Event) gethcoretypes.Bloom {
	var bin gethcoretypes.Bloom
	for _, event := range events {
		if len(event.Type) == 0 {
			continue
		}
		for _, attr := range event.Attributes {
			if len(attr.Key) == 0 {
				continue
			} else if attr.Index {
				bin.Add(eventFilter(event.Type, attr.Key, attr.Value))
			}
		}
	}
	return bin
}

func filtersFromConditions(conditions []syntax.Condition) ([][]byte, error) {
	var filters [][]byte
	for _, c := range conditions {
		if c.Tag == types.BlockHeightKey {
			continue
		} else if c.Tag == types.TxHeightKey {
			return nil, fmt.Errorf("tx height is not allowed in the query")
		}

		if c.Op == syntax.TEq {
			filter := fmt.Appendf(nil, "%s=%s", c.Tag, c.Arg.Value())
			filters = append(filters, filter)
		} else {
			return nil, fmt.Errorf("unsupported operation: %s", c.Op)
		}
	}
	return filters, nil
}

func bloomKeyForBlock(height int64) []byte {
	return fmt.Appendf(nil, "%s/%d",
		blockBloomKeyPrefix,
		int64ToBytes(height),
	)
}

func bloomKeyForSectionIndex(section int64, index int64) []byte {
	return fmt.Appendf(nil, "%s/%d/%d",
		sectionBloomKeyPrefix,
		int64ToBytes(section),
		int64ToBytes(index),
	)
}
