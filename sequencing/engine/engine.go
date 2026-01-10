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
	cmttypes "github.com/cometbft/cometbft/types"
)

type p2pMsg struct {
	msg      types.Message
	envelope p2p.Envelope
}

type conflictingCommit struct {
	peerID p2p.ID
	commit *cmttypes.Commit
}

type Engine struct {
	logger  log.Logger
	reactor types.Reactor
	cfg     config.SequencingConfig

	privValidatorPubKey crypto.PubKey
	privValidator       cmttypes.PrivValidator // for signing votes

	state   *sm.State
	stateMu sync.Mutex

	stopOnce *sync.Once
	stopCh   chan struct{}

	// lock to protect proxy app executions
	execMu     sync.Mutex
	blockExec  *sm.BlockExecutor
	blockStore *store.BlockStore
	eventBus   cmttypes.BlockEventPublisher

	chainID                 string
	lastProposedBlockHeight int64
	lastProposedBlockTime   time.Time
	lastProposedBlockNumTxs int

	// only used for graceful shutdown to apply last proposed block
	lastProposedBlock *types.ProposedBlock

	appliedCh          chan struct{}
	receiveCh          chan p2pMsg
	conflictingVotesCh chan conflictingCommit

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

	done chan struct{}
}

func NewEngine(
	logger log.Logger,
	reactor types.Reactor,
	cfg config.SequencingConfig,
	state *sm.State,
	privValidator cmttypes.PrivValidator,
	blockExec *sm.BlockExecutor,
	blockStore *store.BlockStore,
	eventBus cmttypes.BlockEventPublisher,
) *Engine {
	pubkey, err := privValidator.GetPubKey()
	if err != nil {
		panic(err)
	}

	eng := Engine{
		reactor: reactor,
		logger:  logger,
		cfg:     cfg,

		state: state,

		privValidator:       privValidator,
		privValidatorPubKey: pubkey,

		stopOnce:           &sync.Once{},
		stopCh:             make(chan struct{}),
		appliedCh:          make(chan struct{}, 1),
		receiveCh:          make(chan p2pMsg, 100),
		conflictingVotesCh: make(chan conflictingCommit, 4),

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

		done: make(chan struct{}),
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

func (e *Engine) enqueueConflictingCommit(pid p2p.ID, commit *cmttypes.Commit) {
	if commit == nil {
		return
	}
	select {
	case e.conflictingVotesCh <- conflictingCommit{peerID: pid, commit: commit}:
	default:
		e.logger.Debug("conflicting vote queue full; dropping commit", "peer", pid, "height", commit.Height)
	}
}

// SetMetrics overrides the Engine metrics. Passing nil resets metrics to no-ops.
func (e *Engine) SetMetrics(m *Metrics) {
	if m == nil {
		e.metrics = NopMetrics()
	} else {
		e.metrics = m
	}
}

func (e *Engine) Start() error {
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(8)

	go func() {
		defer waitGroup.Done()
		e.blockProcessor()
	}()
	go func() {
		defer waitGroup.Done()
		e.attesterCommitProcessor()
	}()
	go func() {
		defer waitGroup.Done()
		e.statusProcessor()
	}()
	go func() {
		defer waitGroup.Done()
		e.proposerProcessor()
	}()
	go func() {
		defer waitGroup.Done()
		e.attestorProcessor()
	}()
	go func() {
		defer waitGroup.Done()
		e.badPeerCleanup()
	}()
	go func() {
		defer waitGroup.Done()
		e.receiveRoutine()
	}()
	go func() {
		defer waitGroup.Done()
		e.conflictingVoteProcessor()
	}()

	go func() {
		waitGroup.Wait()
		close(e.done)
	}()
	return nil
}

func (e *Engine) Stop() error {
	e.stopOnce.Do(func() {
		close(e.stopCh)
	})
	e.metrics.Syncing.Set(0)
	return nil
}

func (e *Engine) Wait() {
	<-e.done

	// apply last proposed block if not applied yet
	e.stateMu.Lock()
	stateHeight := e.state.LastBlockHeight
	lastProposedBlockHeight := e.lastProposedBlockHeight
	lastProposedBlock := e.lastProposedBlock
	e.stateMu.Unlock()

	if lastProposedBlockHeight == stateHeight+1 {
		_, _, _ = e.applyProposedBlock(lastProposedBlock)
	}
}

// ResetState replaces the engine's working state and synchronizes related metadata.
func (e *Engine) ResetState(state sm.State) {
	e.stateMu.Lock()
	*e.state = state
	e.stateMu.Unlock()

	e.switchRole()
}

func (e *Engine) AddPeer(peer p2p.Peer) {}

func (e *Engine) RemovePeer(peer p2p.Peer, reason any) {
	e.peerSet.Remove(peer.ID())
	e.badPeers.Delete(peer.ID())
	e.blockBucket.RemovePeer(peer.ID())
	e.commitBucket.RemovePeer(peer.ID())
}

func (e *Engine) flagBadPeer(pid p2p.ID, reason string) {
	e.badPeers.Store(pid, time.Now())
	e.peerSet.Remove(pid)
	e.blockBucket.RemovePeer(pid)
	e.commitBucket.RemovePeer(pid)
	e.logger.Error("flagged bad peer", "peer", pid, "reason", reason)
}

func (e *Engine) receiveRoutine() {
	for {
		select {
		case <-e.stopCh:
			return
		case p2pMsg := <-e.receiveCh:
			msg := p2pMsg.msg
			envelope := p2pMsg.envelope

			// skip it if the peer is bad
			if _, ok := e.badPeers.Load(envelope.Src.ID()); ok {
				continue
			}

			switch m := msg.(type) {
			case *types.StatusUpdate:
				e.handleStatusUpdate(envelope.Src, m)
			case *types.BlockRequest:
				e.handleBlockRequest(envelope.Src, m)
			case *types.BlockResponse:
				e.handleBlockResponse(envelope.Src, m)
			default:
				e.logger.Debug("pool received unhandled message: %T", msg)
			}
		}
	}
}

func (e *Engine) Receive(msg types.Message, envelope p2p.Envelope) {
	if e.receiveCh != nil {
		e.receiveCh <- p2pMsg{msg: msg, envelope: envelope}
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

	e.switchRoleLocked()
}

// switchRoleLocked checks the validator set and updates whether we are a sequencer or attestor
func (e *Engine) switchRoleLocked() {
	_, val := e.state.Validators.GetByAddress(e.privValidatorPubKey.Address())
	if val != nil && val.VotingPower == cmttypes.SequencerVotingPower {
		e.isSequencer.Store(true)
		e.isAttestor.Store(false)
	} else if val != nil && val.VotingPower == cmttypes.AttestorVotingPower {
		e.isAttestor.Store(true)
		e.isSequencer.Store(false)
	} else {
		e.isAttestor.Store(false)
		e.isSequencer.Store(false)
	}
}
