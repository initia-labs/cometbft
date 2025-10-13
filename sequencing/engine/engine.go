package engine

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/sequencing/types"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	comettypes "github.com/cometbft/cometbft/types"
)

type Engine struct {
	logger  log.Logger
	reactor types.Reactor
	cfg     config.SequencingConfig

	privValidatorPubKey crypto.PubKey
	privValidator       comettypes.PrivValidator // for signing votes

	state   *sm.State
	stateMu *sync.Mutex

	stopOnce *sync.Once
	stopCh   chan struct{}

	blockExec  *sm.BlockExecutor
	blockStore *store.BlockStore
	eventBus   comettypes.BlockEventPublisher

	chainID                 string
	lastProposedBlockHeight int64
	lastProposedBlockTime   time.Time
	lastProposedBlockNumTxs int
	appliedCh               chan struct{}

	// peer management
	peerSet  *types.PeerSet
	badPeers *sync.Map // p2p.ID -> struct{}

	// bucket management
	blockBucket   *types.P2PBucket[*types.ProposedBlock]
	commitBucket  *types.P2PBucket[*types.AttestorCommit]
	requestWindow *types.BlockRequestTracker

	metrics *Metrics

	eventBusRegistered bool

	isSequencer *atomic.Bool
	isAttestor  *atomic.Bool
}

func NewEngine(
	logger log.Logger,
	reactor types.Reactor,
	cfg config.SequencingConfig,
	state *sm.State,
	privValidator comettypes.PrivValidator,
	blockExec *sm.BlockExecutor,
	blockStore *store.BlockStore,
	eventBus comettypes.BlockEventPublisher,
) *Engine {
	pubkey, err := privValidator.GetPubKey()
	if err != nil {
		panic(err)
	}

	eng := Engine{
		reactor: reactor,
		logger:  logger,
		cfg:     cfg,

		state:   state,
		stateMu: &sync.Mutex{},

		privValidator:       privValidator,
		privValidatorPubKey: pubkey,

		stopOnce:  &sync.Once{},
		stopCh:    make(chan struct{}),
		appliedCh: make(chan struct{}, 1),

		blockExec:  blockExec,
		blockStore: blockStore,
		eventBus:   eventBus,

		chainID: state.ChainID,

		peerSet:       types.NewPeerSet(),
		badPeers:      &sync.Map{},
		blockBucket:   types.NewP2PBucket[*types.ProposedBlock](),
		commitBucket:  types.NewP2PBucket[*types.AttestorCommit](),
		requestWindow: types.NewBlockRequestTracker(maxRequestsPerHeight*maxFutureBlocks, blockRequestTimeout),

		metrics: NopMetrics(),

		isSequencer: &atomic.Bool{},
		isAttestor:  &atomic.Bool{},
	}

	eng.switchRole()

	return &eng
}

func (e *Engine) tryRegisterEventBus() {
	if e.eventBusRegistered || e.eventBus == nil {
		return
	}

	if !e.CatchUp() {
		e.blockExec.SetEventBus(e.eventBus)
		e.eventBusRegistered = true
	}
}

func (e *Engine) signalBlockApplied() {
	select {
	case e.appliedCh <- struct{}{}:
	default:
	}
}

// SetMetrics overrides the Engine metrics. Passing nil resets metrics to no-ops.
func (b *Engine) SetMetrics(m *Metrics) {
	if m == nil {
		b.metrics = NopMetrics()
	} else {
		b.metrics = m
	}
}

func (b *Engine) Start() error {
	go b.blockProcessor()
	go b.attesterCommitProcessor()
	go b.statusProcessor()
	go b.proposerProcessor()
	go b.attestorProcessor()
	go b.badPeerCleanup()

	return nil
}

func (b *Engine) Stop() error {
	b.stopOnce.Do(func() {
		close(b.stopCh)
	})
	b.metrics.Syncing.Set(0)
	return nil
}

// ResetState replaces the engine's working state and synchronizes related metadata.
func (e *Engine) ResetState(state sm.State) {
	e.stateMu.Lock()
	*e.state = state
	e.stateMu.Unlock()

	e.switchRole()
}

func (p *Engine) AddPeer(peer p2p.Peer) {}

func (p *Engine) RemovePeer(peer p2p.Peer, reason any) {
	p.peerSet.Remove(peer.ID())
	p.badPeers.Delete(peer.ID())
	p.blockBucket.RemovePeer(peer.ID())
	p.commitBucket.RemovePeer(peer.ID())
}

func (p *Engine) flagBadPeer(pid p2p.ID, reason string) {
	p.badPeers.Store(pid, time.Now())
	p.peerSet.Remove(pid)
	p.blockBucket.RemovePeer(pid)
	p.commitBucket.RemovePeer(pid)
	p.logger.Error("flagged bad peer", "peer", pid, "reason", reason)
}

func (p *Engine) Receive(msg types.Message, envelope p2p.Envelope) {
	if _, ok := p.badPeers.Load(envelope.Src.ID()); ok {
		return
	}

	switch m := msg.(type) {
	case *types.StatusUpdate:
		p.handleStatusUpdate(envelope.Src, m)
	case *types.BlockRequest:
		p.handleBlockRequest(envelope.Src, m)
	case *types.BlockResponse:
		p.handleBlockResponse(envelope.Src, m)
	default:
		p.logger.Debug("pool received unhandled message: %T", msg)
	}
}

const CatchUpThreshold = 10

// CatchUp returns true if we are catching up to the network.
// We consider ourselves to be catching up if our block store
// is more than CatchUpThreshold blocks behind the highest
// known block among our peers.
func (e *Engine) CatchUp() bool {
	if e.blockStore != nil {
		storeHeight := e.blockStore.Height()
		topHeight, ok := e.peerSet.TopHeight()
		if ok && storeHeight+CatchUpThreshold < topHeight {
			return true
		}
	}

	return false
}

// switchRole checks the validator set and updates whether we are a sequencer or attestor
func (e *Engine) switchRole() {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()

	_, val := e.state.Validators.GetByAddress(e.privValidatorPubKey.Address())
	if val != nil && val.VotingPower == comettypes.SequencerVotingPower {
		e.isSequencer.Store(true)
		e.isAttestor.Store(false)
	} else if val != nil && val.VotingPower == comettypes.AttestorVotingPower {
		e.isAttestor.Store(true)
		e.isSequencer.Store(false)
	} else {
		e.isAttestor.Store(false)
		e.isSequencer.Store(false)
	}
}
