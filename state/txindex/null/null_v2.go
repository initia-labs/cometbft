package null

import (
	"context"
	"errors"

	"github.com/cometbft/cometbft/libs/log"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	"github.com/cometbft/cometbft/state/txindex"
)

var _ txindex.TxIndexerV2 = (*TxIndexV2)(nil)

// TxIndex acts as a /dev/null.
type TxIndexV2 struct{}

// Get on a TxIndex is disabled and panics when invoked.
func (txi *TxIndexV2) Get(_ []byte) (*abci.TxResult, error) {
	return nil, errors.New(`indexing is disabled (set 'tx_index = "kv"' in config)`)
}

// AddBatch is a noop and always returns nil.
func (txi *TxIndexV2) AddBatch(_ *txindex.Batch) error {
	return nil
}

// Index is a noop and always returns nil.
func (txi *TxIndexV2) Index(_ *abci.TxResult) error {
	return nil
}

func (txi *TxIndexV2) Search(_ context.Context, _ *query.Query, _ int64) (chan abci.TxResult, chan error) {
	txs := make(chan abci.TxResult)
	errs := make(chan error)
	close(txs)
	close(errs)
	return txs, errs
}

func (txi *TxIndexV2) SetLogger(log.Logger) {

}

func (txi *TxIndexV2) Prune(curHeight int64) error {
	return nil
}

func (txi *TxIndexV2) StartMigration() {
}

func (txi *TxIndexV2) FinishMigration(endHeight int64) error {
	return nil
}

func (txi *TxIndexV2) MigrationHeight() (int64, error) {
	return 0, nil
}
