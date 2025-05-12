package core

import (
	"errors"
	"fmt"
	"sort"

	abci "github.com/cometbft/cometbft/abci/types"
	cmtmath "github.com/cometbft/cometbft/libs/math"
	cmtquery "github.com/cometbft/cometbft/libs/pubsub/query"
	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	rpctypes "github.com/cometbft/cometbft/rpc/jsonrpc/types"
	"github.com/cometbft/cometbft/state/txindex/null"
	"github.com/cometbft/cometbft/types"
)

const (
	maxTotalCount = 1000
)

// Tx allows you to query the transaction results. `nil` could mean the
// transaction is in the mempool, invalidated, or was not sent in the first
// place.
// More: https://docs.cometbft.com/v0.38.x/rpc/#/Info/tx
func (env *Environment) Tx(_ *rpctypes.Context, hash []byte, prove bool) (*ctypes.ResultTx, error) {
	// if index is disabled, return error
	if _, ok := env.TxIndexer.(*null.TxIndex); ok {
		return nil, fmt.Errorf("transaction indexing is disabled")
	}

	r, err := env.TxIndexer.Get(hash)
	if err != nil {
		return nil, err
	}

	if r == nil {
		return nil, fmt.Errorf("tx (%X) not found", hash)
	}

	var proof types.TxProof
	if prove {
		block := env.BlockStore.LoadBlock(r.Height)
		if block != nil {
			proof = block.Data.Txs.Proof(int(r.Index))
		}
	}

	return &ctypes.ResultTx{
		Hash:     hash,
		Height:   r.Height,
		Index:    r.Index,
		TxResult: r.Result,
		Tx:       r.Tx,
		Proof:    proof,
	}, nil
}

// TxSearch allows you to query for multiple transactions results. It returns a
// list of transactions (maximum ?per_page entries) and the total count.
// More: https://docs.cometbft.com/v0.38.x/rpc/#/Info/tx_search
func (env *Environment) TxSearch(
	ctx *rpctypes.Context,
	query string,
	prove bool,
	pagePtr, perPagePtr *int,
	orderBy string,
) (*ctypes.ResultTxSearch, error) {
	if !env.TxIndexerV2.IsMigrating() {
		return env.txSearchV2(ctx, query, prove, pagePtr, perPagePtr, orderBy)
	}
	return env.txSearch(ctx, query, prove, pagePtr, perPagePtr, orderBy)
}

func (env *Environment) txSearch(
	ctx *rpctypes.Context,
	query string,
	prove bool,
	pagePtr, perPagePtr *int,
	orderBy string,
) (*ctypes.ResultTxSearch, error) {
	// if index is disabled, return error
	if _, ok := env.TxIndexer.(*null.TxIndex); ok {
		return nil, errors.New("transaction indexing is disabled")
	} else if len(query) > maxQueryLength {
		return nil, errors.New("maximum query length exceeded")
	}

	q, err := cmtquery.New(query)
	if err != nil {
		return nil, err
	}

	results, err := env.TxIndexer.Search(ctx.Context(), q)
	if err != nil {
		return nil, err
	}

	// sort results (must be done before pagination)
	switch orderBy {
	case "desc":
		sort.Slice(results, func(i, j int) bool {
			if results[i].Height == results[j].Height {
				return results[i].Index > results[j].Index
			}
			return results[i].Height > results[j].Height
		})
	case "asc", "":
		sort.Slice(results, func(i, j int) bool {
			if results[i].Height == results[j].Height {
				return results[i].Index < results[j].Index
			}
			return results[i].Height < results[j].Height
		})
	default:
		return nil, errors.New("expected order_by to be either `asc` or `desc` or empty")
	}

	// paginate results
	totalCount := len(results)
	perPage := env.validatePerPage(perPagePtr)

	page, err := validatePage(pagePtr, perPage, totalCount)
	if err != nil {
		return nil, err
	}

	skipCount := validateSkipCount(page, perPage)
	pageSize := cmtmath.MinInt(perPage, totalCount-skipCount)

	apiResults := make([]*ctypes.ResultTx, 0, pageSize)
	for i := skipCount; i < skipCount+pageSize; i++ {
		r := results[i]

		var proof types.TxProof
		if prove {
			block := env.BlockStore.LoadBlock(r.Height)
			if block != nil {
				proof = block.Data.Txs.Proof(int(r.Index))
			}
		}

		apiResults = append(apiResults, &ctypes.ResultTx{
			Hash:     types.Tx(r.Tx).Hash(),
			Height:   r.Height,
			Index:    r.Index,
			TxResult: r.Result,
			Tx:       r.Tx,
			Proof:    proof,
		})
	}

	return &ctypes.ResultTxSearch{Txs: apiResults, TotalCount: totalCount}, nil
}

func (env *Environment) txSearchV2(
	ctx *rpctypes.Context,
	query string,
	prove bool,
	pagePtr, perPagePtr *int,
	orderBy string,
) (*ctypes.ResultTxSearch, error) {
	// if index is disabled, return error
	if _, ok := env.TxIndexerV2.(*null.TxIndexV2); ok {
		return nil, errors.New("transaction indexing is disabled")
	} else if len(query) > maxQueryLength {
		return nil, errors.New("maximum query length exceeded")
	}

	if orderBy == "desc" {
		return nil, errors.New("order_by is not supported")
	}

	q, err := cmtquery.New(query)
	if err != nil {
		return nil, err
	}

	resultChan, errChan := env.TxIndexerV2.Search(ctx.Context(), q, maxTotalCount)

	perPage := env.validatePerPage(perPagePtr)
	page := 1
	if pagePtr != nil {
		page = *pagePtr
	}
	if page <= 0 {
		return nil, fmt.Errorf("page should be greater than 0")
	} else if page*perPage > maxTotalCount {
		return nil, fmt.Errorf("page size is too large, max count is %d", maxTotalCount)
	}

	results := make([]*ctypes.ResultTx, 0, perPage)
	totalCount := 0

	// cache for block and response
	type cache struct {
		block    *types.Block
		response *abci.ResponseFinalizeBlock
	}

	// use cache to avoid loading the same block and response multiple times
	blockCache := make(map[int64]cache)
RESULT_LOOP:
	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				break RESULT_LOOP
			}
			totalCount++
			if totalCount > maxTotalCount {
				break RESULT_LOOP
			} else if totalCount <= (page-1)*perPage || totalCount > page*perPage {
				continue
			}

			var block *types.Block
			var response *abci.ResponseFinalizeBlock
			if c, ok := blockCache[result.Height]; ok {
				block = c.block
				response = c.response
			} else {
				block = env.BlockStore.LoadBlock(result.Height)
				if block == nil {
					totalCount--
					continue
				}
				response, err = env.StateStore.LoadFinalizeBlockResponse(result.Height)
				if err != nil || response == nil {
					totalCount--
					continue
				}

				blockCache[result.Height] = cache{
					block:    block,
					response: response,
				}
			}

			var proof types.TxProof
			if prove {
				proof = block.Data.Txs.Proof(int(result.Index))
			}

			results = append(results, &ctypes.ResultTx{
				Hash:     block.Data.Txs[result.Index].Hash(),
				Height:   result.Height,
				Index:    result.Index,
				TxResult: *response.TxResults[result.Index],
				Tx:       block.Data.Txs[result.Index],
				Proof:    proof,
			})
		case err := <-errChan:
			if err != nil {
				return nil, err
			}
		}
	}

	return &ctypes.ResultTxSearch{Txs: results, TotalCount: totalCount}, nil
}
