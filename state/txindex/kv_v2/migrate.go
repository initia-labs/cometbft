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

func (txi *TxIndex) FinishMigration() error {
	txi.isMigrating = false
	return txi.SetMigrationHeight(math.MaxInt64)
}

func (txi *TxIndex) IsMigrating() bool {
	return txi.isMigrating
}
