package mempool

import (
	"errors"
	"sync"
	"time"

	"fmt"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	protomem "github.com/cometbft/cometbft/proto/tendermint/mempool"
	"github.com/cometbft/cometbft/types"
)

const (
	regossipInterval = 500 * time.Millisecond
)

// Reactor handles mempool tx broadcasting amongst peers.
// It is driven by application events pushed through the ProxyMempool's event channel.
type Reactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	mempool *ProxyMempool
	ids     *mempoolIDs

	// peerKnownTxs tracks which txs each peer already knows about
	peerKnownTxsMtx sync.RWMutex
	peerKnownTxs    map[p2p.ID]map[types.TxKey]struct{}

	// insertedTxs is the regossip set, txs promoted to the active mempool
	insertedTxsMtx sync.RWMutex
	insertedTxs    map[types.TxKey]types.Tx

	// lastKnownHeight tracks the last height seen by regossipLoop
	lastKnownHeight int64
}

// NewReactor returns a new Reactor with the given config and mempool.
func NewReactor(config *cfg.MempoolConfig, mempool *ProxyMempool) *Reactor {
	memR := &Reactor{
		config:       config,
		mempool:      mempool,
		ids:          newMempoolIDs(),
		peerKnownTxs: make(map[p2p.ID]map[types.TxKey]struct{}),
		insertedTxs:  make(map[types.TxKey]types.Tx),
	}
	memR.BaseReactor = *p2p.NewBaseReactor("Mempool", memR)

	return memR
}

// InitPeer implements Reactor by creating a state for the peer.
func (memR *Reactor) InitPeer(peer p2p.Peer) p2p.Peer {
	memR.ids.ReserveForPeer(peer)
	return peer
}

// SetLogger sets the Logger on the reactor and the underlying mempool.
func (memR *Reactor) SetLogger(l log.Logger) {
	memR.Logger = l
	memR.mempool.SetLogger(l)
}

// OnStart implements p2p.BaseReactor.
func (memR *Reactor) OnStart() error {
	if !memR.config.Broadcast {
		memR.Logger.Info("Tx broadcasting is disabled")
	}

	// start the global event goroutines
	go memR.appEventLoop()
	if memR.config.Broadcast {
		go memR.regossipLoop()
	}

	return nil
}

// GetChannels implements Reactor by returning the list of channels for this
// reactor.
func (memR *Reactor) GetChannels() []*p2p.ChannelDescriptor {
	largestTx := make([]byte, memR.config.MaxTxBytes)
	batchMsg := protomem.Message{
		Sum: &protomem.Message_Txs{
			Txs: &protomem.Txs{Txs: [][]byte{largestTx}},
		},
	}

	return []*p2p.ChannelDescriptor{
		{
			ID:                  MempoolChannel,
			Priority:            5,
			RecvMessageCapacity: batchMsg.Size(),
			MessageType:         &protomem.Message{},
		},
	}
}

// AddPeer implements Reactor.
func (memR *Reactor) AddPeer(peer p2p.Peer) {
	memR.peerKnownTxsMtx.Lock()
	memR.peerKnownTxs[peer.ID()] = make(map[types.TxKey]struct{})
	memR.peerKnownTxsMtx.Unlock()

	// start a routine to check transactions from the peer
	go memR.checkTxRoutine(peer)
}

// RemovePeer implements Reactor.
func (memR *Reactor) RemovePeer(peer p2p.Peer, _ interface{}) {
	memR.ids.Reclaim(peer)

	memR.peerKnownTxsMtx.Lock()
	delete(memR.peerKnownTxs, peer.ID())
	memR.peerKnownTxsMtx.Unlock()
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *Reactor) Receive(e p2p.Envelope) {
	memR.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
	switch msg := e.Message.(type) {
	case *protomem.Txs:
		protoTxs := msg.GetTxs()
		if len(protoTxs) == 0 {
			memR.Logger.Error("received empty txs from peer", "src", e.Src)
			return
		}
		txInfo := TxInfo{SenderID: memR.ids.GetForPeer(e.Src)}
		if e.Src != nil {
			txInfo.SenderP2PID = e.Src.ID()
		}

		var err error
		for _, tx := range protoTxs {
			ntx := types.Tx(tx)
			err = memR.mempool.CheckTx(ntx, nil, txInfo)
			if err != nil {
				switch {
				case errors.Is(err, ErrTxInCache):
					memR.Logger.Debug("Tx already exists in cache", "tx", ntx.String())
				case errors.As(err, &ErrMempoolIsFull{}):
					// using debug level to avoid flooding when traffic is high
					memR.Logger.Debug(err.Error())
				default:
					memR.Logger.Info("Could not check tx", "tx", ntx.String(), "err", err)
				}
			}
		}
	default:
		memR.Logger.Error("unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		return
	}
}

// PeerState describes the state of a peer.
type PeerState interface {
	GetHeight() int64
}

// appEventLoop consumes events from the ProxyMempool's event channel and acts on them.
func (memR *Reactor) appEventLoop() {
	for {
		if !memR.IsRunning() {
			return
		}

		select {
		case ev := <-memR.mempool.AppEventCh():
			switch ev.Type {
			case EventTxQueued:
				if memR.config.Broadcast {
					memR.gossipTxToPeers(ev.TxKey, ev.Tx, ev.SenderID)
				}

			case EventTxInserted:
				memR.insertedTxsMtx.Lock()
				memR.insertedTxs[ev.TxKey] = ev.Tx
				memR.insertedTxsMtx.Unlock()

				memR.mempool.SetHasValidTxs(true)
				memR.mempool.NotifyTxsAvailable()

				if memR.config.Broadcast {
					memR.gossipTxToPeers(ev.TxKey, ev.Tx, ev.SenderID)
				}

			case EventTxRemoved:
				memR.insertedTxsMtx.Lock()
				delete(memR.insertedTxs, ev.TxKey)
				memR.insertedTxsMtx.Unlock()

				memR.mempool.RemoveTxByKey(ev.TxKey)

				memR.peerKnownTxsMtx.Lock()
				for _, known := range memR.peerKnownTxs {
					delete(known, ev.TxKey)
				}
				memR.peerKnownTxsMtx.Unlock()
			}

		case <-memR.Quit():
			return
		}
	}
}

// regossipLoop periodically regossips inserted txs to peers that may not have them yet.
func (memR *Reactor) regossipLoop() {
	ticker := time.NewTicker(regossipInterval)
	defer ticker.Stop()

	for {
		if !memR.IsRunning() {
			return
		}

		select {
		case <-ticker.C:
			// on each new block, clear peerKnownTxs to bound memory growth
			if h := memR.mempool.Height(); h > memR.lastKnownHeight {
				memR.lastKnownHeight = h
				memR.peerKnownTxsMtx.Lock()
				for pid := range memR.peerKnownTxs {
					memR.peerKnownTxs[pid] = make(map[types.TxKey]struct{})
				}
				memR.peerKnownTxsMtx.Unlock()
			}

			// clear committed txs and snapshot the regossip set
			memR.insertedTxsMtx.Lock()
			txs := make(map[types.TxKey]types.Tx, len(memR.insertedTxs))
			for k, v := range memR.insertedTxs {
				if memR.mempool.IsIncludedTx(v) {
					delete(memR.insertedTxs, k)
				} else {
					txs[k] = v
				}
			}
			memR.insertedTxsMtx.Unlock()

			for txKey, tx := range txs {
				memR.gossipTxToPeers(txKey, tx, "")
			}

		case <-memR.Quit():
			return
		}
	}
}

// gossipTxToPeers sends a tx to all peers that don't already know about it, skipping the original sender.
func (memR *Reactor) gossipTxToPeers(txKey types.TxKey, tx types.Tx, excludePeer p2p.ID) {
	peers := memR.Switch.Peers().List()

	memR.peerKnownTxsMtx.Lock()
	defer memR.peerKnownTxsMtx.Unlock()

	for _, peer := range peers {
		pid := peer.ID()
		if pid == excludePeer {
			continue
		}

		known, ok := memR.peerKnownTxs[pid]
		if !ok {
			continue
		}
		if _, already := known[txKey]; already {
			continue
		}

		if peer.Send(p2p.Envelope{
			ChannelID: MempoolChannel,
			Message:   &protomem.Txs{Txs: [][]byte{tx}},
		}) {
			known[txKey] = struct{}{}
		}
	}
}

func (memR *Reactor) checkTxRoutine(peer p2p.Peer) {
	peerID := memR.ids.GetForPeer(peer)
	checkTxChan, ok := memR.ids.GetCheckTxChan(peer)
	if !ok {
		memR.Logger.Debug("skipping checkTxRoutine; peer channel missing", "peer", peer.ID())
		return
	}

	txInfo := TxInfo{SenderID: peerID, SenderP2PID: peer.ID()}

	for {
		if !memR.IsRunning() || !peer.IsRunning() {
			return
		}

		select {
		case protoTxs := <-checkTxChan:
			for _, tx := range protoTxs {
				ntx := types.Tx(tx)

				// record peerKnownTxs before calling CheckTx
				txKey := ntx.Key()
				memR.peerKnownTxsMtx.Lock()
				if known, exists := memR.peerKnownTxs[peer.ID()]; exists {
					known[txKey] = struct{}{}
				}
				memR.peerKnownTxsMtx.Unlock()

				err := memR.mempool.CheckTx(ntx, nil, txInfo)
				if err != nil {
					switch {
					case errors.Is(err, ErrTxInCache):
						memR.Logger.Debug("Tx already exists in cache", "tx", ntx.String())
					case errors.As(err, &ErrMempoolIsFull{}):
						// using debug level to avoid flooding when traffic is high
						memR.Logger.Debug(err.Error())
					default:
						memR.Logger.Info("Could not check tx", "tx", ntx.String(), "err", err)
					}
				}
			}
		case <-peer.Quit():
			return
		case <-memR.Quit():
			return
		}
	}
}


// TxsMessage is a Message containing transactions.
type TxsMessage struct {
	Txs []types.Tx
}

// String returns a string representation of the TxsMessage.
func (m *TxsMessage) String() string {
	return fmt.Sprintf("[TxsMessage %v]", m.Txs)
}
