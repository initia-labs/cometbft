package kv

import "math"

func (txi *TxIndex) MigrationHeight() (int64, error) {
	migrationHeight, err := txi.store.Get([]byte(migrationKey))
	if err != nil {
		return 0, err
	} else if migrationHeight == nil {
		return 0, nil
	}
	return int64FromBytes(migrationHeight), nil
}

func (txi *TxIndex) SetMigrationHeight(height int64) error {
	return txi.store.Set([]byte(migrationKey), int64ToBytes(height))
}

func (txi *TxIndex) StartMigration() {
	txi.isMigrating = true
}

func (txi *TxIndex) FinishMigration(endHeight int64) error {
	if !txi.isMigrating {
		return nil
	}

	sectionIndex := (endHeight + (bloomSectionSize - 1)) / bloomSectionSize
	storeBatch := txi.store.NewBatch()
	defer func() {
		storeBatch.Close()
		txi.isMigrating = false
	}()
	err := txi.createSectionBloom(sectionIndex, storeBatch)
	if err != nil {
		return err
	}

	err = storeBatch.Set([]byte(migrationKey), int64ToBytes(math.MaxInt64))
	if err != nil {
		return err
	}
	return storeBatch.WriteSync()
}
