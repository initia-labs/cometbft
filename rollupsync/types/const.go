package types

const ChainNameL1 string = "initia"
const ChainNameCelestia string = "celestia"

type SyncMode uint8

const (
	SyncModeDefault SyncMode = iota
	SyncModeChallenge
)

func SyncModeFromString(modeStr string) SyncMode {
	switch modeStr {
	case "challenge":
		return SyncModeChallenge
	}
	return SyncModeDefault
}

func (sm SyncMode) String() string {
	switch sm {
	case SyncModeDefault:
		return "sync"
	case SyncModeChallenge:
		return "challenge"
	}
	return "unknown"
}
