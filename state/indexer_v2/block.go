package indexer

import (
	"context"

	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/pubsub/query"
	"github.com/cometbft/cometbft/types"
)

//go:generate ../../scripts/mockery_generate.sh BlockIndexer

// BlockIndexer defines an interface contract for indexing block events.
type BlockIndexer interface {
	// Has returns true if the given height has been indexed. An error is returned
	// upon database query failure.
	Has(height int64) (bool, error)

	// Index indexes FinalizeBlock events for a given block by its height.
	Index(types.EventDataNewBlockEvents) error

	// Search performs a query for block heights that match a given FinalizeBlock
	// event search criteria.
	Search(ctx context.Context, q *query.Query, maxCount int64) (chan int64, chan error)

	SetLogger(l log.Logger)

	// Prune removes all block indexes below a certain height.
	Prune(curHeight int64) error

	// StartMigration starts the migration process.
	StartMigration()

	// FinishMigration finalizes the migration process.
	FinishMigration() error

	// SetMigrationHeight sets the migration height to the given height.
	SetMigrationHeight(height int64) error

	// MigrationHeight returns the height of the migration.
	MigrationHeight() (int64, error)

	// IsMigrating returns true if the migration is active.
	IsMigrating() bool

	// NotifyNewBlock notifies the indexer that a new block has been added.
	NotifyNewBlock()
}
