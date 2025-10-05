package types

import "github.com/cometbft/cometbft/p2p"

type Reactor interface {
	Peers() []p2p.Peer
	SelfID() p2p.ID
	PeerIDs() []p2p.ID
	Peer(p2p.ID) p2p.Peer
	MempoolSize() int
	TxsAvailable() <-chan struct{}
}
