package types

import (
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/types"
)

type Reactor interface {
	Peers() []p2p.Peer
	SelfID() p2p.ID
	PeerIDs() []p2p.ID
	Peer(p2p.ID) p2p.Peer
	MempoolSize() int
	TxsAvailable() <-chan struct{}

	// reports conflicting votes to the evidence pool to be processed into evidence
	ReportConflictingVotes(height int64, blockID types.BlockID, valAddr types.Address, valIdx int32, sig1, sig2 types.ExtendedCommitSig)
}
