package types

const (
	ProposeChannel = byte(0x40)
	AttestChannel  = byte(0x41)
	SyncChannel    = byte(0x42) // shared for status updates, block requests/responses
)
