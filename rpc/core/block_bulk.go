package core

import (
	"fmt"

	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	rpctypes "github.com/cometbft/cometbft/rpc/jsonrpc/types"
)

// hard limit to the number of blocks that can be queried at once
const MaxBlockRange = 100

// BlockBulk return invalid block information
func (env *Environment) BlockBulk(ctx *rpctypes.Context, start, end int64) (*ctypes.ResultBlockBulk, error) {
	if start > end {
		return nil, fmt.Errorf("start height is greater than end height")
	}
	if end-start >= MaxBlockRange {
		return nil, fmt.Errorf("block range exceeds maximum limit %d", MaxBlockRange)
	}

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
