package core

import (
	"fmt"

	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	rpctypes "github.com/cometbft/cometbft/rpc/jsonrpc/types"
)

// BlockBulk return invalid block information
func (env *Environment) BlockBulk(ctx *rpctypes.Context, start, end int64) (*ctypes.ResultBlockBulk, error) {
	blocks := make([][]byte, end-start+1)
	for i := start; i <= end; i++ {
		bz := env.BlockStore.LoadBlockBytes(i)
		if bz == nil {
			return nil, fmt.Errorf("block at height `%d` not found", i)
		}

		blocks[i-start] = bz
	}

	return &ctypes.ResultBlockBulk{Blocks: blocks}, nil
}
