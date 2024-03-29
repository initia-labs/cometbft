package types

import "github.com/cometbft/cometbft/types"

type BatchInfo struct {
	L1QueryHeight int64
	Batch         []byte
}

type BlockInfo struct {
	L1QueryHeight int64
	Block         *types.Block
}
