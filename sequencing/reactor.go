package sequencing

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/p2p"
	seqproto "github.com/cometbft/cometbft/proto/tendermint/sequencing"
	seqengine "github.com/cometbft/cometbft/sequencing/engine"
	seqtypes "github.com/cometbft/cometbft/sequencing/types"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
)

// interface to the evidence pool
type evidencePool interface {
	// reports conflicting votes to the evidence pool to be processed into evidence
	ReportConflictingVotes(voteA, voteB *types.Vote)
}

type ReactorConfig struct {
	State        sm.State
	BlockExec    *sm.BlockExecutor
	BlockStore   *store.BlockStore
	PrivVal      types.PrivValidator
	Mempool      mempool.Mempool
	Config       *config.SequencingConfig
	Metrics      *seqengine.Metrics
	EventBus     types.BlockEventPublisher
	EvidencePool evidencePool
}

type Reactor struct {
	p2p.BaseReactor

	ep     evidencePool
	engine *seqengine.Engine

	logger log.Logger

	mempool          mempool.Mempool
	txsAvailableCh   <-chan struct{}
	txsAvailableOnce sync.Once

	deferredStart atomic.Bool
	startMu       sync.Mutex
	engineRunning bool
	engineReady   atomic.Bool
}

func NewReactor(cfg ReactorConfig, logger log.Logger) (*Reactor, error) {
	if cfg.BlockExec == nil {
		return nil, fmt.Errorf("sequencing reactor requires block executor")
	}
	if cfg.BlockStore == nil {
		return nil, fmt.Errorf("sequencing reactor requires block store")
	}
	if cfg.PrivVal == nil {
		return nil, fmt.Errorf("sequencing reactor requires private validator")
	}

	seqCfg := cfg.Config
	if seqCfg == nil {
		seqCfg = config.DefaultSequencingConfig()
	}
	if err := seqCfg.ValidateBasic(); err != nil {
		return nil, fmt.Errorf("invalid sequencing config: %w", err)
	}

	stateCopy := cfg.State
	statePtr := &stateCopy

	r := &Reactor{
		logger:  logger,
		mempool: cfg.Mempool,
		ep:      cfg.EvidencePool,
	}
	if r.logger == nil {
		r.logger = log.NewNopLogger()
	}

	r.BaseReactor = *p2p.NewBaseReactor("Sequencing", r)
	r.BaseService.Logger = r.logger

	eng := seqengine.NewEngine(r.logger, r, *seqCfg, statePtr, cfg.PrivVal, cfg.BlockExec, cfg.BlockStore, cfg.EventBus)
	r.engine = eng
	if cfg.Metrics != nil {
		r.engine.SetMetrics(cfg.Metrics)
	}

	return r, nil
}

func (r *Reactor) OnStart() error {
	if r.engine == nil {
		return fmt.Errorf("sequencing reactor missing engine")
	}
	if r.deferredStart.Load() {
		return nil
	}
	if err := r.startEngine(); err != nil {
		return err
	}
	r.engineReady.Store(true)
	return nil
}

func (r *Reactor) OnStop() {
	if r.engine == nil {
		return
	}
	if err := r.stopEngine(); err != nil {
		r.logger.Error("failed to stop sequencing engine", "err", err)
	}
}

func (r *Reactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID:                  seqtypes.ProposeChannel,
			Priority:            10,
			SendQueueCapacity:   100,
			RecvBufferCapacity:  50 * 4096,
			RecvMessageCapacity: 4 * 1024 * 1024,
			MessageType:         &seqproto.Message{},
		},
		{
			ID:                  seqtypes.AttestChannel,
			Priority:            8,
			SendQueueCapacity:   100,
			RecvBufferCapacity:  10 * 1024,
			RecvMessageCapacity: 2 * 1024 * 1024,
			MessageType:         &seqproto.Message{},
		},
		{
			ID:                  seqtypes.SyncChannel,
			Priority:            5,
			SendQueueCapacity:   20,
			RecvBufferCapacity:  10 * 1024,
			RecvMessageCapacity: 4 * 1024 * 1024,
			MessageType:         &seqproto.Message{},
		},
	}
}

func (r *Reactor) Receive(e p2p.Envelope) {
	if !r.IsRunning() {
		return
	}
	if !r.engineReady.Load() {
		return
	}
	if r.engine == nil {
		return
	}

	msgProto, ok := e.Message.(*seqproto.Message)
	if !ok {
		r.logger.Error("unexpected message type", "type", fmt.Sprintf("%T", e.Message))
		r.Switch.StopPeerForError(e.Src, fmt.Errorf("unexpected message type %T", e.Message))
		return
	}

	msg, err := seqtypes.MsgFromProto(msgProto)
	if err != nil {
		r.logger.Error("failed to decode message", "err", err, "peer", e.Src)
		r.Switch.StopPeerForError(e.Src, err)
		return
	}

	r.engine.Receive(msg, e)
}

func (r *Reactor) InitPeer(peer p2p.Peer) p2p.Peer {
	if peer == nil {
		return nil
	}

	peer.Set(types.PeerStateKey, seqtypes.NewPeerHeight())
	return peer
}

func (r *Reactor) AddPeer(peer p2p.Peer) {
	if r.engine == nil {
		return
	}
	r.engine.AddPeer(peer)
}

func (r *Reactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	if r.engine == nil {
		return
	}
	r.engine.RemovePeer(peer, reason)
}

func (r *Reactor) SelfID() p2p.ID {
	return r.Switch.NodeInfo().ID()
}

func (r *Reactor) Peers() []p2p.Peer {
	return r.Switch.Peers().List()
}

func (r *Reactor) PeerIDs() []p2p.ID {
	peers := r.Switch.Peers().List()
	ids := make([]p2p.ID, 0, len(peers))
	for _, p := range peers {
		ids = append(ids, p.ID())
	}
	return ids
}

func (r *Reactor) Peer(id p2p.ID) p2p.Peer {
	return r.Switch.Peers().Get(id)
}

func (r *Reactor) MempoolSize() int {
	if r.mempool == nil {
		return 0
	}
	return r.mempool.Size()
}

// SetDeferredStart toggles deferred engine start. It must be invoked before the
// reactor is started by the switch.
func (r *Reactor) SetDeferredStart(deferStart bool) {
	r.deferredStart.Store(deferStart)
	if deferStart {
		r.engineReady.Store(false)
	}
}

// Enable starts the sequencing engine when deferred start is enabled.
func (r *Reactor) Enable() error {
	if err := r.startEngine(); err != nil {
		return err
	}
	r.deferredStart.Store(false)
	r.engineReady.Store(true)
	return nil
}

func (r *Reactor) startEngine() error {
	r.startMu.Lock()
	defer r.startMu.Unlock()
	if r.engineRunning {
		return nil
	}
	if err := r.engine.Start(); err != nil {
		return err
	}
	r.engineRunning = true
	return nil
}

func (r *Reactor) stopEngine() error {
	r.startMu.Lock()
	defer r.startMu.Unlock()
	if !r.engineRunning {
		return nil
	}
	r.engineReady.Store(false)
	if err := r.engine.Stop(); err != nil {
		return err
	}
	r.engineRunning = false

	r.engine.Wait()
	return nil
}

// UpdateState replaces the engine's working state with the provided value.
func (r *Reactor) UpdateState(state sm.State) {
	if r.engine == nil {
		return
	}
	r.engine.ResetState(state)
}

// TxsAvailable lazily initializes and returns the mempool's TxsAvailable channel.
func (r *Reactor) TxsAvailable() <-chan struct{} {
	if r.mempool == nil {
		return nil
	}
	r.txsAvailableOnce.Do(func() {
		r.mempool.EnableTxsAvailable()
		r.txsAvailableCh = r.mempool.TxsAvailable()
	})
	return r.txsAvailableCh
}

func (r *Reactor) SetMetrics(metrics *seqengine.Metrics) {
	if r.engine == nil {
		return
	}
	r.engine.SetMetrics(metrics)
}

func (r *Reactor) CatchUp() bool {
	if r.engine == nil {
		return false
	}

	return r.engine.CatchUp()
}

// ReportConflictingVotes reports conflicting votes to the evidence pool to be processed into evidence
func (r *Reactor) ReportConflictingVotes(vote1, vote2 *types.Vote) {
	if r.ep != nil {
		r.ep.ReportConflictingVotes(vote1, vote2)
	}
}
