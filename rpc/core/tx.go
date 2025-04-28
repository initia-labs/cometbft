package core

import (
	"errors"
	"fmt"

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
	// if index is disabled, return error
	if _, ok := env.TxIndexer.(*null.TxIndex); ok {
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

	resultChan, errChan := env.TxIndexer.Search(ctx.Context(), q, maxTotalCount)

	perPage := env.validatePerPage(perPagePtr)
	page := *pagePtr
	if page <= 0 {
		return nil, fmt.Errorf("page should be greater than 0")
	} else if page*perPage > maxTotalCount {
		return nil, fmt.Errorf("page size is too large, max count is %d", maxTotalCount)
	}

	results := make([]*ctypes.ResultTx, 0, perPage)
	totalCount := 0

RESULT_LOOP:
	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				break RESULT_LOOP
			}
			totalCount++
			if totalCount >= maxTotalCount {
				break RESULT_LOOP
			} else if totalCount <= (page-1)*perPage || totalCount > page*perPage {
				continue
			}

			block := env.BlockStore.LoadBlock(result.Height)
			if block == nil {
				return nil, fmt.Errorf("block not found")
			}
			response, err := env.StateStore.LoadFinalizeBlockResponse(result.Height)
			if err != nil {
				return nil, err
			}

			var proof types.TxProof
			if prove {
				proof = block.Data.Txs.Proof(int(result.Index))
			}

			results = append(results, &ctypes.ResultTx{
				Hash:     types.Tx(result.Tx).Hash(),
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
