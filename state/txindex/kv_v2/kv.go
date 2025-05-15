package kv

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	"golang.org/x/sync/errgroup"

	"github.com/cosmos/gogoproto/proto"

	dbm "github.com/cometbft/cometbft-db"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	"github.com/cometbft/cometbft/libs/pubsub/query/syntax"
	indexerv2 "github.com/cometbft/cometbft/state/indexer_v2"
	"github.com/cometbft/cometbft/state/txindex"
	"github.com/cometbft/cometbft/types"

	"github.com/ethereum/go-ethereum/core/bloombits"
	gethcoretypes "github.com/ethereum/go-ethereum/core/types"

	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
)

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

	// isMigrating is true if the indexer is migrating from the old indexer to the new one.
	isMigrating bool

	newBlockNotifier    chan int64
	sectionBloomRunning atomic.Bool
}

// NewTxIndex creates new KV indexer.
func NewTxIndex(store dbm.DB, blockStore *store.BlockStore, stateStore sm.Store, retainHeight int64) *TxIndex {
	txi := &TxIndex{
		store:        store,
		log:          log.NewNopLogger(),
		blockStore:   blockStore,
		stateStore:   stateStore,
		retainHeight: retainHeight,

		newBlockNotifier: make(chan int64),
	}
	txi.sectionBloomRunning.Store(false)
	go txi.startSectionBloomCreation()

	return txi
}

func (txi *TxIndex) SetLogger(l log.Logger) {
	txi.log = l
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

// AddBatch indexes a batch of transactions using the given list of events. Each
// key that indexed from the tx's events is a composite of the event type and
// the respective attribute's key delimited by a "." (eg. "account.number").
// Any event with an empty type is not indexed.
//
// The following is indexed:
//
// block bloom: encode(bb | height) => block bloom
// section bloom: encode(sb | sectionIndex) => section bloom
func (txi *TxIndex) AddBatch(b *txindex.Batch) error {
	storeBatch := txi.store.NewBatch()
	defer storeBatch.Close()

	if len(b.Ops) == 0 {
		return nil
	}

	blockHeight := b.Ops[0].Height
	for _, result := range b.Ops {
		hash := types.Tx(result.Tx).Hash()

		// index by height (always)
		err := storeBatch.Set(keyForHeight(result), hash)
		if err != nil {
			return err
		}

		result.Result = abci.ExecTxResult{}
		result.Tx = nil

		rawBytes, err := proto.Marshal(result)
		if err != nil {
			return err
		}
		// index by hash (always)
		err = storeBatch.Set(hash, rawBytes)
		if err != nil {
			return err
		}
	}

	// update block bloom
	blockBloom := bloomForBlock(b.Ops)
	err := storeBatch.Set(bloomKeyForBlock(blockHeight), blockBloom[:])
	if err != nil {
		return err
	}

	base, err := txi.Base()
	if err != nil {
		return err
	} else if base == 0 || base > blockHeight {
		err = storeBatch.Set([]byte(baseKey), int64ToBytes(blockHeight))
		if err != nil {
			return err
		}
	}

	// store last indexed height
	height, err := txi.Height()
	if err != nil {
		return err
	} else if height < blockHeight {
		err = storeBatch.Set([]byte(heightKey), int64ToBytes(blockHeight))
		if err != nil {
			return err
		}
	}

	return storeBatch.WriteSync()
}

func (txi *TxIndex) NotifyNewBlock(height int64) {
	// if the section bloom is not running, start it and update the flag
	if txi.sectionBloomRunning.CompareAndSwap(false, true) {
		txi.newBlockNotifier <- height
	}
}

// startSectionBloomCreation creates a section bloom for the given height in a separate goroutine.
func (txi *TxIndex) startSectionBloomCreation() {
	logger := txi.log.With("function", "SectionBloomCreation")

	creationFn := func(height int64) {
		// reset the flag when the function is done
		defer txi.sectionBloomRunning.Store(false)

		dbSectionIndex, err := txi.SectionIndex()
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
		batch := txi.store.NewBatch()
		nextSectionIndex := dbSectionIndex + 1
		if nextSectionIndex == 0 {
			nextSectionIndex = sectionIndex
		}
		err = txi.createSectionBloom(nextSectionIndex, batch)
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

	for height := range txi.newBlockNotifier {
		creationFn(height)
	}
}

func (txi *TxIndex) createSectionBloom(sectionIndex int64, batch dbm.Batch) error {
	gen, err := bloombits.NewGenerator(uint(bloomSectionSize))
	if err != nil {
		return err
	}

	for i := range bloomSectionSize {
		blockBloom, err := txi.store.Get(bloomKeyForBlock(sectionIndex*bloomSectionSize + i))
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
func (txi *TxIndex) Search(ctx context.Context, q *query.Query, maxCount int64) (chan abci.TxResult, chan error) {
	resultChan := make(chan abci.TxResult)
	errChan := make(chan error)

	go func() {
		defer func() {
			close(resultChan)
			close(errChan)
		}()

		errChan <- txi.search(ctx, q, maxCount, resultChan)
	}()
	return resultChan, errChan
}

func (txi *TxIndex) search(ctx context.Context, q *query.Query, maxCount int64, resultChan chan abci.TxResult) error {
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
			resultChan <- *res
			return nil
		}
	} else if txi.isMigrating {
		return fmt.Errorf("indexer is migrating, only tx hash search is supported")
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

	filters, err := filtersFromConditions(conditions)
	if err != nil {
		return err
	}

	begin := int64(1)
	height, err := txi.Height()
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

	idxBase, err := txi.Base()
	if err != nil {
		return err
	}
	begin = max(begin, idxBase, txi.blockStore.Base())

	// if the begin is greater than the end, return nil
	if begin > end {
		return nil
	}

	sectionIndex, err := txi.SectionIndex()
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
							sectionBitbloom, err := txi.store.Get(bloomKeyForSectionIndex(int64(section), int64(task.Bit)))
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

		for i := 0; i < bloomFilterThreads; i++ {
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
				results, err := txi.checkMatch(int64(number), filters)
				if err != nil {
					return err
				}
				for _, result := range results {
					innerCountForIndexed++
					resultChan <- result
				}
			}
		}
		if err != nil {
			return err
		}

		begin = max(begin, endForIndexed+1)
	}

	// for unindexed events

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

	resultsArray := make([][]abci.TxResult, batchNum)
	for i := int64(0); i < batchNum; i++ {
		// make local copy of i for goroutine
		idx := i
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
				events, err := txi.checkMatch(batchNumber, filters)
				if err != nil {
					return err
				}
				innerCountForUnindexed.Add(int64(len(events)))
				if innerCountForUnindexed.Load()+innerCountForIndexed >= maxCount {
					return errors.New("too many results, reduce the query range")
				}
				resultsArray[idx] = append(resultsArray[idx], events...)
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

func (txi *TxIndex) checkMatch(number int64, filters [][]byte) ([]abci.TxResult, error) {
	results := make([]abci.TxResult, 0)

	res, err := txi.stateStore.LoadFinalizeBlockResponse(number)
	if err != nil {
		return nil, nil
	}

	for txIndex, txResult := range res.TxResults {
		matchCount := 0
	TXCHECK_LOOP:
		for _, conditionFilter := range filters {
			for _, event := range txResult.Events {
				if len(event.Type) == 0 {
					continue
				}
				for _, attr := range event.Attributes {
					if len(attr.Key) == 0 {
						continue
					} else if attr.Index {
						filter := eventFilter(event.Type, attr.Key, attr.Value)
						if bytes.Equal(conditionFilter, filter) {
							matchCount++
							continue TXCHECK_LOOP
						}
					}
				}
			}
			// no match found
			break
		}

		if matchCount == len(filters) {
			results = append(results, abci.TxResult{
				Height: number,
				Index:  uint32(txIndex),
			})
		}
	}
	return results, nil
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

func (txi *TxIndex) Base() (int64, error) {
	base, err := txi.store.Get([]byte(baseKey))
	if err != nil {
		return 0, err
	} else if base == nil {
		return 0, nil
	}
	return int64FromBytes(base), nil
}

func (txi *TxIndex) Height() (int64, error) {
	height, err := txi.store.Get([]byte(heightKey))
	if err != nil {
		return 0, err
	} else if height == nil {
		return 0, nil
	}
	return int64FromBytes(height), nil
}

func (txi *TxIndex) SectionIndex() (int64, error) {
	sectionIndex, err := txi.store.Get([]byte(sectionIndexKey))
	if err != nil {
		return 0, err
	} else if sectionIndex == nil {
		return -1, nil
	}
	return int64FromBytes(sectionIndex), nil
}

func (txi *TxIndex) Prune(curHeight int64) error {
	minHeight := curHeight - txi.retainHeight
	if minHeight <= 0 || minHeight >= curHeight {
		return nil
	}

	pruneBatch := txi.store.NewBatch()
	defer pruneBatch.Close()

	base, err := txi.Base()
	if err != nil {
		return err
	}

	// end key is exclusive
	iter, err := txi.store.Iterator(bloomKeyForBlock(base), bloomKeyForBlock(minHeight+1))
	if err != nil {
		return err
	}
	defer iter.Close()

	for ; iter.Valid(); iter.Next() {
		if err := pruneBatch.Delete(iter.Key()); err != nil {
			return err
		}
	}

	iter2, err := txi.store.Iterator(bloomKeyForSectionIndex(base/bloomSectionSize, 0), bloomKeyForSectionIndex(minHeight/bloomSectionSize, bloomSectionSize))
	if err != nil {
		return err
	}
	defer iter2.Close()

	for ; iter2.Valid(); iter2.Next() {
		if err := pruneBatch.Delete(iter2.Key()); err != nil {
			return err
		}
	}

	iter3, err := txi.store.Iterator(keyForHeight(&abci.TxResult{Height: base}), keyForHeight(&abci.TxResult{Height: minHeight + 1}))
	if err != nil {
		return err
	}
	defer iter3.Close()

	for ; iter3.Valid(); iter3.Next() {
		if err := pruneBatch.Delete(iter3.Key()); err != nil {
			return err
		}

		// tx hash
		if err := pruneBatch.Delete(iter3.Value()); err != nil {
			return err
		}
	}

	err = pruneBatch.Set([]byte(baseKey), int64ToBytes(minHeight+1))
	if err != nil {
		return err
	}
	return pruneBatch.WriteSync()
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

func bloomForBlock(results []*abci.TxResult) gethcoretypes.Bloom {
	var bin gethcoretypes.Bloom
	for _, result := range results {
		for _, event := range result.Result.Events {
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
	}
	return bin
}

func filtersFromConditions(conditions []syntax.Condition) ([][]byte, error) {
	var filters [][]byte
	for _, c := range conditions {
		if c.Tag == types.TxHeightKey {
			continue
		} else if c.Tag == types.BlockHeightKey {
			return nil, fmt.Errorf("block height is not allowed in the query")
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
	return fmt.Appendf(nil, "%s/%s",
		blockBloomKeyPrefix,
		int64ToBytes(height),
	)
}

func bloomKeyForSectionIndex(section int64, index int64) []byte {
	return fmt.Appendf(nil, "%s/%s/%s",
		sectionBloomKeyPrefix,
		int64ToBytes(section),
		int64ToBytes(index),
	)
}
