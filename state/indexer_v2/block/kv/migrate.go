package kv

import "math"

// StartMigration sets the indexer to migration mode.
func (idx *BlockerIndexer) StartMigration() {
	idx.isMigrating = true
}

// SetMigrationHeight sets the migration height to the given height.
func (idx *BlockerIndexer) SetMigrationHeight(height int64) error {
	return idx.store.Set([]byte(migrationKey), int64ToBytes(height))
}

// FinishMigration finalizes the migration process by:
// 1. Re-creating a section bloom filter for the given end height because the old indexer might have not created it
// 2. Setting the migration height to int64 max to prevent any future migrations
// 3. Disabling migration mode on the indexer
func (idx *BlockerIndexer) FinishMigration(endHeight int64) error {
	if !idx.isMigrating {
		return nil
	}

	sectionIndex := (endHeight + (bloomSectionSize - 1)) / bloomSectionSize
	batch := idx.store.NewBatch()
	defer func() {
		batch.Close()
		idx.isMigrating = false
	}()
	err := idx.createSectionBloom(sectionIndex, batch)
	if err != nil {
		return err
	}
	err = batch.Set([]byte(migrationKey), int64ToBytes(math.MaxInt64))
	if err != nil {
		return err
	}
	return batch.WriteSync()
}

// MigrationHeight returns the height of the migration.
func (idx *BlockerIndexer) MigrationHeight() (int64, error) {
	migrationHeight, err := idx.store.Get([]byte(migrationKey))
	if err != nil {
		return 0, err
	} else if migrationHeight == nil {
		return 0, nil
	}
	return int64FromBytes(migrationHeight), nil
}

func (idx *BlockerIndexer) IsMigrating() bool {
	return idx.isMigrating
}
