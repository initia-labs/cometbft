package types

import "github.com/cometbft/cometbft/types"

type BatchInfo struct {
	BatchChainHeight int64
	Batch            []byte
}

type BlockInfo struct {
	BatchChainHeight int64
	Block            *types.Block
}
