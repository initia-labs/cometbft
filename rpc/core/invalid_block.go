package core

import (
	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	rpctypes "github.com/cometbft/cometbft/rpc/jsonrpc/types"
)

// InvalidBlock return invalid block information
func (env *Environment) InvalidBlock(ctx *rpctypes.Context) (*ctypes.ResultInvalidBlock, error) {
	reason, height := env.BlockStore.LoadInvalidBlock()
	return &ctypes.ResultInvalidBlock{Reason: reason, Height: height}, nil
}
