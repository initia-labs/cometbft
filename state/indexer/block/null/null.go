package null

import (
	"context"
	"errors"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	"github.com/cometbft/cometbft/state/indexer"
	"github.com/cometbft/cometbft/types"
)

var _ indexer.BlockIndexer = (*BlockerIndexer)(nil)

// TxIndex implements a no-op block indexer.
type BlockerIndexer struct{}

func (idx *BlockerIndexer) Has(int64) (bool, error) {
	return false, errors.New(`indexing is disabled (set 'tx_index = "kv"' in config)`)
}

func (idx *BlockerIndexer) Index(types.EventDataNewBlockEvents) error {
	return nil
}

func (idx *BlockerIndexer) Search(context.Context, *query.Query, int64) (chan int64, chan error) {
	resultChan := make(chan int64)
	errorChan := make(chan error)

	go func() {
		defer close(resultChan)
		defer close(errorChan)
	}()
	return resultChan, errorChan
}

func (idx *BlockerIndexer) SetLogger(log.Logger) {
}

func (idx *BlockerIndexer) Prune(curHeight int64) error {
	return nil
}

func (idx *BlockerIndexer) StartReindex() {
}

func (idx *BlockerIndexer) FinalizeReindex(startHeight, endHeight int64) error {
	return nil
}
