package types

const CHAIN_NAME_L1 = "l1"
const CHAIN_NAME_CELESTIA = "celestia"

type SyncMode uint8

const (
	SyncModeDefault SyncMode = iota
	SyncModeChallenge
)
