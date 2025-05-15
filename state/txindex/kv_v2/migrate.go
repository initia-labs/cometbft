package kv

import "math"

// MigrationHeight returns the height of the migration.
func (txi *TxIndex) MigrationHeight() (int64, error) {
	migrationHeight, err := txi.store.Get([]byte(migrationKey))
	if err != nil {
		return 0, err
	} else if migrationHeight == nil {
		return 0, nil
	}
	return int64FromBytes(migrationHeight), nil
}

// SetMigrationHeight sets the migration height to the given height.
func (txi *TxIndex) SetMigrationHeight(height int64) error {
	return txi.store.Set([]byte(migrationKey), int64ToBytes(height))
}

// StartMigration sets the indexer to migration mode.
func (txi *TxIndex) StartMigration() {
	txi.isMigrating = true
}

// FinishMigration sets the indexer to not migrating mode and sets the migration height to the max int64.
func (txi *TxIndex) FinishMigration() error {
	txi.isMigrating = false
	return txi.SetMigrationHeight(math.MaxInt64 - 1)
}

// IsMigrating returns true if the indexer is in migration mode.
func (txi *TxIndex) IsMigrating() bool {
	return txi.isMigrating
}
