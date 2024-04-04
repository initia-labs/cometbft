package types

import (
	"fmt"

	"github.com/cometbft/cometbft/types"
)

// BatchInfoUpdates is a list of BatchInfoUpdate
type BatchInfoUpdates []BatchInfoUpdate

// String returns a string representation of BatchInfoUpdates
func (b BatchInfoUpdates) String() string {
	res := ""
	for _, update := range b {
		res += fmt.Sprintf("| %d ~ %d: %s, %s ", update.Start, update.End, update.Chain, update.Submitter)
	}
	return res
}

// BatchInfoUpdate is a struct that contains information about a batch update
type BatchInfoUpdate struct {
	// Chain - need to fetch batch data from this chain
	Chain string
	// Submitter - need to fetch batch data filtered by this submitter
	Submitter string
	// The starting height of this batch update applied
	Start int64
	// The ending height of this batch update applied
	End int64
}

// BatchChanInfo is a struct that contains information about a data required
// for batch data channel.
type BatchChanInfo struct {
	// BatchChainHeight is the height that has already been checked to
	// search for the batch submitter's transaction.
	BatchChainHeight int64
	// Batch is the batch data
	Batch []byte
}

// BlockChanInfo is a struct that contains information about a data required
// for block data channel.
type BlockChanInfo struct {
	// BatchChainHeight is the height that has already been checked to
	// search for the batch submitter's transaction.
	BatchChainHeight int64
	// Block is the block data
	Block *types.Block
	// Commit is the commit data
	Commit *types.Commit
}
