package mempool

import (
	"errors"
	"time"

	"fmt"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	cmtsync "github.com/cometbft/cometbft/libs/sync"
	"github.com/cometbft/cometbft/p2p"
	protomem "github.com/cometbft/cometbft/proto/tendermint/mempool"
	"github.com/cometbft/cometbft/types"
)

const (
	regossipCheckInterval = 1 * time.Second
	regossipBaseInterval  = 3 * time.Second
	regossipMaxInterval   = 5 * time.Minute
	regossipMaxAttempts   = 16

	regossipHeightInterval    int64 = 3
	regossipMaxHeightInterval int64 = 100
)

// regossipEntry tracks a tx in the regossip set with per-tx back-off state.
type regossipEntry struct {
	tx               types.Tx
	lastGossipTime   time.Time
	lastGossipHeight int64
	attempts         int
}

// Reactor handles mempool tx broadcasting amongst peers.
// It is driven by application events pushed through the ProxyMempool's event channel.
type Reactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	mempool *ProxyMempool
	ids     *mempoolIDs

	// insertedTxs is the regossip set of txs promoted to the active mempool.
	insertedTxsMtx cmtsync.Mutex
	insertedTxs    map[types.TxKey]*regossipEntry
}

// NewReactor returns a new Reactor with the given config and mempool.
func NewReactor(config *cfg.MempoolConfig, mempool *ProxyMempool) *Reactor {
	memR := &Reactor{
		config:      config,
		mempool:     mempool,
		ids:         newMempoolIDs(),
		insertedTxs: make(map[types.TxKey]*regossipEntry),
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
	// start a routine to check transactions from the peer
	go memR.checkTxRoutine(peer)
}

// RemovePeer implements Reactor.
func (memR *Reactor) RemovePeer(peer p2p.Peer, _ interface{}) {
	memR.ids.Reclaim(peer)
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

		// send the transactions to the checkTxRoutine
		checkTxChan, ok := memR.ids.GetCheckTxChan(e.Src)
		if !ok {
			memR.Logger.Debug("dropping txs; peer channel missing", "src", e.Src)
			return
		}

		checkTxChan <- protoTxs
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
					memR.gossipTxToPeers(ev.Tx, ev.SenderID)
				}

			case EventTxInserted:
				memR.insertedTxsMtx.Lock()
				memR.insertedTxs[ev.TxKey] = &regossipEntry{
					tx:               ev.Tx,
					lastGossipTime:   time.Now(),
					lastGossipHeight: memR.mempool.Height(),
				}
				memR.mempool.SetHasValidTxs(true)
				memR.mempool.NotifyTxsAvailable()
				memR.insertedTxsMtx.Unlock()

			case EventTxRemoved:
				memR.insertedTxsMtx.Lock()
				delete(memR.insertedTxs, ev.TxKey)
				if len(memR.insertedTxs) == 0 {
					memR.mempool.SetHasValidTxs(false)
				}
				memR.insertedTxsMtx.Unlock()

				memR.mempool.RemoveTxByKey(ev.TxKey)
			}

		case <-memR.Quit():
			return
		}
	}
}

// regossipLoop periodically checks inserted txs and regossips those whose
// per-tx back-off interval has elapsed. Both time and height back-off are
// exponential: base * 2^attempts (capped at their respective maximums).
func (memR *Reactor) regossipLoop() {
	ticker := time.NewTicker(regossipCheckInterval)
	defer ticker.Stop()

	for {
		if !memR.IsRunning() {
			return
		}

		select {
		case now := <-ticker.C:
			curHeight := memR.mempool.Height()
			var toGossip []types.Tx

			memR.insertedTxsMtx.Lock()
			for k, entry := range memR.insertedTxs {
				if memR.mempool.IsIncludedTx(entry.tx) {
					delete(memR.insertedTxs, k)
					continue
				}

				backoff := min(regossipBaseInterval<<min(entry.attempts, regossipMaxAttempts), regossipMaxInterval)
				timeDue := now.Sub(entry.lastGossipTime) >= backoff

				heightBackoff := min(regossipHeightInterval<<min(entry.attempts, regossipMaxAttempts), regossipMaxHeightInterval)
				heightDue := curHeight >= entry.lastGossipHeight+heightBackoff

				if timeDue || heightDue {
					toGossip = append(toGossip, entry.tx)
					entry.lastGossipTime = now
					entry.lastGossipHeight = curHeight
					entry.attempts++
				}
			}
			memR.insertedTxsMtx.Unlock()

			for _, item := range toGossip {
				memR.gossipTxToPeers(item, "")
			}

		case <-memR.Quit():
			return
		}
	}
}

// gossipTxToPeers sends a tx to all connected peers, skipping the excluding peer.
func (memR *Reactor) gossipTxToPeers(tx types.Tx, excludePeer p2p.ID) {
	for _, peer := range memR.Switch.Peers().List() {
		if peer.ID() == excludePeer {
			continue
		}

		peer.Send(p2p.Envelope{
			ChannelID: MempoolChannel,
			Message:   &protomem.Txs{Txs: [][]byte{tx}},
		})
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
