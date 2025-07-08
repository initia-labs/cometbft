package kv

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/cometbft/cometbft/libs/log"

	"github.com/cosmos/gogoproto/proto"

	dbm "github.com/cometbft/cometbft-db"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	"github.com/cometbft/cometbft/libs/pubsub/query/syntax"
	"github.com/cometbft/cometbft/state/bloombits"
	indexerv2 "github.com/cometbft/cometbft/state/indexer_v2"
	"github.com/cometbft/cometbft/state/txindex"
	"github.com/cometbft/cometbft/types"

	"github.com/cometbft/cometbft/state/filtermaps"

	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
)

var _ txindex.FiltermapTxIndexer = (*TxIndex)(nil)

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

	filtermap *filtermaps.FilterMaps
}

// NewTxIndex creates new KV indexer.
func NewTxIndex(store dbm.DB, blockStore *store.BlockStore, stateStore sm.Store, retainHeight int64) *TxIndex {
	fm := filtermaps.NewFilterMaps(store, blockStore, stateStore, 0, filtermaps.DefaultParams, filtermaps.Config{
		History:        10000,
		Disabled:       false,
		ExportFileName: "",
		HashScheme:     false,
	})

	return &TxIndex{
		store:        store,
		log:          log.NewNopLogger(),
		blockStore:   blockStore,
		stateStore:   stateStore,
		retainHeight: retainHeight,
		filtermap:    fm,
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

func (txi *TxIndex) NotifyNewBlock(height int64) {
	txi.filtermap.SetBlockProcessing(false)
	txi.filtermap.SetTarget(uint64(height-1), 0)
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
	height := txi.blockStore.Height()

	backend := txi.filtermap.NewMatcherBackend()

	txEvents, err := filtermaps.GetPotentialMatches(ctx, txi.log, backend, uint64(begin), uint64(height), filters)
	if err != nil {
		return err
	}

	for _, txEvent := range txEvents {
		resultChan <- abci.TxResult{
			Height: txEvent.BlockNumber,
			Index:  uint32(txEvent.TxIndex),
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

func eventFilter(eventType string, attrKey string, attrValue string) []byte {
	return fmt.Appendf(nil, "%s.%s=%s", eventType, attrKey, attrValue)
}

func bloomForBlock(results []*abci.TxResult) bloombits.Bloom {
	var bin bloombits.Bloom
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
