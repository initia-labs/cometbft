package http

import (
	"context"

	ctypes "github.com/cometbft/cometbft/rpc/core/types"
)

func (c *baseRPCClient) CelestiaTxSearch(
	ctx context.Context,
	query string,
	prove bool,
	page,
	perPage *int,
	orderBy string,
) (*ctypes.CelestiaResultTxSearch, error) {
	result := new(ctypes.CelestiaResultTxSearch)
	params := map[string]interface{}{
		"query":    query,
		"prove":    prove,
		"order_by": orderBy,
	}

	if page != nil {
		params["page"] = page
	}
	if perPage != nil {
		params["per_page"] = perPage
	}

	_, err := c.caller.Call(ctx, "tx_search", params, result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
