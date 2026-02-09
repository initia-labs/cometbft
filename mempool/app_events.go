package mempool

import (
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/types"
)

// AppMempoolEventType describes the kind of event the application mempool is pushing.
type AppMempoolEventType int

const (
	// EventTxQueued means the tx was accepted into the app's pending queue.
	EventTxQueued AppMempoolEventType = iota
	// EventTxInserted means the tx was inserted to the mempool.
	EventTxInserted
	// EventTxRemoved means the tx was evicted, expired, or replaced.
	EventTxRemoved
)

// AppMempoolEvent is pushed by the application into the ProxyMempool's event channel for the reactor
type AppMempoolEvent struct {
	Type     AppMempoolEventType
	TxKey    types.TxKey
	Tx       types.Tx // full bytes for gossip, nil for removed
	SenderID p2p.ID   // original peer sender
}

// EventProvider is implemented by mempools that expose an event channel
// for app to reactor communication. The cosmos-sdk server uses this to pass
// the channel to the application after CometBFT node startup.
type EventProvider interface {
	AppEventCh() chan AppMempoolEvent
}
