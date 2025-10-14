package engine

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	gogoproto "github.com/cosmos/gogoproto/proto"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/service"
	mpmocks "github.com/cometbft/cometbft/mempool/mocks"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	seqproto "github.com/cometbft/cometbft/proto/tendermint/sequencing"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/proxy"
	seqtypes "github.com/cometbft/cometbft/sequencing/types"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	cmttypes "github.com/cometbft/cometbft/types"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testApp is a lightweight ABCI application used for integration-style tests.
type testApp struct {
	abci.BaseApplication

	CommitVotes      []abci.VoteInfo
	Misbehavior      []abci.Misbehavior
	LastTime         time.Time
	ValidatorUpdates []abci.ValidatorUpdate
	AppHash          []byte

	applyDelay    time.Duration
	commitHistory map[int64][]abci.VoteInfo
}

func (app *testApp) FinalizeBlock(_ context.Context, req *abci.RequestFinalizeBlock) (*abci.ResponseFinalizeBlock, error) {
	if app.applyDelay > 0 {
		time.Sleep(app.applyDelay)
	}
	app.CommitVotes = req.DecidedLastCommit.Votes
	app.Misbehavior = req.Misbehavior
	app.LastTime = req.Time

	if app.commitHistory != nil && len(req.DecidedLastCommit.Votes) > 0 {
		votes := make([]abci.VoteInfo, len(req.DecidedLastCommit.Votes))
		copy(votes, req.DecidedLastCommit.Votes)
		app.commitHistory[req.Height] = votes
	}

	txResults := make([]*abci.ExecTxResult, len(req.Txs))
	for i := range req.Txs {
		txResults[i] = &abci.ExecTxResult{Code: abci.CodeTypeOK}
	}

	return &abci.ResponseFinalizeBlock{
		ValidatorUpdates: app.ValidatorUpdates,
		ConsensusParamUpdates: &cmtproto.ConsensusParams{
			Version: &cmtproto.VersionParams{App: 1},
		},
		TxResults: txResults,
		AppHash:   app.AppHash,
	}, nil
}

func (app *testApp) Commit(_ context.Context, _ *abci.RequestCommit) (*abci.ResponseCommit, error) {
	return &abci.ResponseCommit{RetainHeight: 1}, nil
}

func (app *testApp) PrepareProposal(_ context.Context, req *abci.RequestPrepareProposal) (*abci.ResponsePrepareProposal, error) {
	txs := make([][]byte, 0, len(req.Txs))
	var total int64
	for _, tx := range req.Txs {
		if len(tx) == 0 {
			continue
		}
		total += int64(len(tx))
		if total > req.MaxTxBytes {
			break
		}
		txs = append(txs, tx)
	}
	return &abci.ResponsePrepareProposal{Txs: txs}, nil
}

func (app *testApp) ProcessProposal(_ context.Context, req *abci.RequestProcessProposal) (*abci.ResponseProcessProposal, error) {
	for _, tx := range req.Txs {
		if len(tx) == 0 {
			return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_REJECT}, nil
		}
	}
	return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_ACCEPT}, nil
}

// networkPeer is a lightweight in-memory p2p.Peer implementation that routes
// envelopes directly to the target node's engine.
type networkPeer struct {
	*service.BaseService

	id          p2p.ID
	node        *testNode
	counterpart *networkPeer

	data          sync.Map
	removalFailed bool
}

type testReactor struct {
	mu    sync.RWMutex
	self  p2p.ID
	peers map[p2p.ID]*networkPeer

	txsAvailable chan struct{}
}

func newTestReactor(self p2p.ID) *testReactor {
	return &testReactor{
		self:         self,
		peers:        make(map[p2p.ID]*networkPeer),
		txsAvailable: make(chan struct{}, 1),
	}
}

func (r *testReactor) Peers() []p2p.Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	peers := make([]p2p.Peer, 0, len(r.peers))
	for _, peer := range r.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (r *testReactor) SelfID() p2p.ID { return r.self }

func (r *testReactor) PeerIDs() []p2p.ID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]p2p.ID, 0, len(r.peers))
	for id := range r.peers {
		ids = append(ids, id)
	}
	return ids
}

func (r *testReactor) Peer(id p2p.ID) p2p.Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.peers[id]
}

func (r *testReactor) MempoolSize() int {
	return 0
}

func (r *testReactor) TxsAvailable() <-chan struct{} {
	return r.txsAvailable
}

func (r *testReactor) addPeer(peer *networkPeer) {
	r.mu.Lock()
	r.peers[peer.ID()] = peer
	r.mu.Unlock()
}

func (r *testReactor) ReportConflictingVotes(height int64, blockID cmttypes.BlockID, valAddr cmttypes.Address, valIdx int32, sig1, sig2 cmttypes.ExtendedCommitSig) {
}

func newNetworkPeer(node *testNode, peerID p2p.ID) *networkPeer {
	p := &networkPeer{
		id:   peerID,
		node: node,
	}
	p.BaseService = service.NewBaseService(nil, "networkPeer", p)
	p.BaseService.Logger = log.NewNopLogger()
	return p
}

func (p *networkPeer) OnStart() error { return nil }

func (p *networkPeer) OnStop() {}

func (p *networkPeer) FlushStop() { _ = p.Stop() }

func (p *networkPeer) ID() p2p.ID { return p.id }

func (p *networkPeer) RemoteIP() net.IP { return net.IPv4(127, 0, 0, 1) }

func (p *networkPeer) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26656}
}

func (p *networkPeer) IsOutbound() bool { return false }

func (p *networkPeer) IsPersistent() bool { return true }

func (p *networkPeer) CloseConn() error { return nil }

func (p *networkPeer) NodeInfo() p2p.NodeInfo {
	return p2p.DefaultNodeInfo{DefaultNodeID: p.id, ListenAddr: "tcp://127.0.0.1:0"}
}

func (p *networkPeer) Status() conn.ConnectionStatus { return conn.ConnectionStatus{} }

func (p *networkPeer) SocketAddr() *p2p.NetAddress {
	addr, _ := p2p.NewNetAddressString(string(p.id) + "@127.0.0.1:0")
	return addr
}

func (p *networkPeer) Send(env p2p.Envelope) bool {
	msgProto, ok := env.Message.(*seqproto.Message)
	if !ok {
		panic("unexpected message type sent through networkPeer")
	}
	msg, err := seqtypes.MsgFromProto(msgProto)
	if err != nil {
		panic(err)
	}

	target := p.node.engine
	if target == nil {
		return false
	}

	incoming := p2p.Envelope{
		ChannelID: env.ChannelID,
		Message:   env.Message,
		Src:       p.counterpart,
	}

	target.Receive(msg, incoming)
	return true
}

func (p *networkPeer) TrySend(env p2p.Envelope) bool {
	return p.Send(env)
}

func (p *networkPeer) Set(key string, value interface{}) {
	p.data.Store(key, value)
}

func (p *networkPeer) Get(key string) interface{} {
	v, ok := p.data.Load(key)
	if !ok {
		return nil
	}
	return v
}

func (p *networkPeer) SetRemovalFailed() { p.removalFailed = true }

func (p *networkPeer) GetRemovalFailed() bool { return p.removalFailed }

// testNode wraps an Engine instance with its supporting infrastructure so that
// nodes can be stopped and restarted during tests.
type testNode struct {
	name  string
	id    p2p.ID
	cfg   config.SequencingConfig
	state *sm.State
	pv    cmttypes.PrivValidator

	stateStore sm.Store
	blockStore *store.BlockStore
	blockExec  *sm.BlockExecutor
	appConns   proxy.AppConns
	mempool    *mpmocks.Mempool
	reactor    *testReactor
	app        *testApp

	engine *Engine
	peers  map[string]*networkPeer
}

type testNodeOptions struct {
	appDelay     time.Duration
	trackCommits bool
}

type testNodeOption func(*testNodeOptions)

func withAppDelay(delay time.Duration) testNodeOption {
	return func(opts *testNodeOptions) {
		opts.appDelay = delay
	}
}

func withCommitTracking() testNodeOption {
	return func(opts *testNodeOptions) {
		opts.trackCommits = true
	}
}

func newTestNode(
	t *testing.T,
	name string,
	baseState sm.State,
	cfg config.SequencingConfig,
	privVal cmttypes.PrivValidator,
	opts ...testNodeOption,
) *testNode {
	t.Helper()

	stateCopy := baseState.Copy()
	statePtr := &stateCopy

	nodeOpts := testNodeOptions{}
	for _, opt := range opts {
		opt(&nodeOpts)
	}

	stateDB := dbm.NewMemDB()
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{DiscardABCIResponses: false})
	require.NoError(t, stateStore.Save(stateCopy))

	blockStore := store.NewBlockStore(dbm.NewMemDB())

	app := &testApp{applyDelay: nodeOpts.appDelay}
	if nodeOpts.trackCommits {
		app.commitHistory = make(map[int64][]abci.VoteInfo)
	}
	appConns := proxy.NewAppConns(proxy.NewLocalClientCreator(app), proxy.NopMetrics())
	require.NoError(t, appConns.Start())

	mempool := &mpmocks.Mempool{}
	mempool.On("ReapMaxBytesMaxGas", mock.Anything, mock.Anything).Return(cmttypes.Txs{}).Maybe()
	mempool.On("Lock").Return().Maybe()
	mempool.On("Unlock").Return().Maybe()
	mempool.On("FlushAppConn").Return(nil).Maybe()
	mempool.On("Update", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mempool.On("Flush").Return().Maybe()
	mempool.On("TxsAvailable").Return((<-chan struct{})(nil)).Maybe()
	mempool.On("EnableTxsAvailable").Return().Maybe()
	mempool.On("Size").Return(0).Maybe()
	mempool.On("SizeBytes").Return(int64(0)).Maybe()
	mempool.On("RemoveTxByKey", mock.Anything).Return(nil).Maybe()
	mempool.On("CheckTx", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mempool.On("ReapMaxTxs", mock.Anything).Return(cmttypes.Txs{}).Maybe()

	logger := log.TestingLogger()

	blockExec := sm.NewBlockExecutor(
		stateStore,
		logger,
		appConns.Consensus(),
		mempool,
		sm.EmptyEvidencePool{},
		blockStore,
	)

	reactor := newTestReactor(p2p.ID(name))
	eng := NewEngine(logger, reactor, cfg, statePtr, privVal, blockExec, blockStore, nil)

	node := &testNode{
		name:       name,
		id:         p2p.ID(name),
		cfg:        cfg,
		state:      statePtr,
		pv:         privVal,
		stateStore: stateStore,
		blockStore: blockStore,
		blockExec:  blockExec,
		appConns:   appConns,
		mempool:    mempool,
		reactor:    reactor,
		app:        app,
		engine:     eng,
		peers:      make(map[string]*networkPeer),
	}

	return node
}

func (n *testNode) start(t *testing.T) {
	t.Helper()
	require.NoError(t, n.engine.Start())
	n.engine.broadcastStatus()
}

func (n *testNode) stop(t *testing.T) {
	t.Helper()
	if n.engine == nil {
		return
	}
	require.NoError(t, n.engine.Stop())
	n.engine = nil
}

func (n *testNode) restart(t *testing.T) {
	t.Helper()
	n.stop(t)
	eng := NewEngine(log.NewNopLogger(), n.reactor, n.cfg, n.state, n.pv, n.blockExec, n.blockStore, nil)
	n.engine = eng
	for _, peer := range n.peers {
		n.engine.AddPeer(peer)
	}
	require.NoError(t, n.engine.Start())
	n.engine.broadcastStatus()
}

func (n *testNode) addPeer(peer *networkPeer) {
	n.peers[string(peer.id)] = peer
	n.reactor.addPeer(peer)
	if n.engine != nil {
		n.engine.AddPeer(peer)
	}
}

func (n *testNode) height() int64 {
	return n.blockStore.Height()
}

func (n *testNode) shutdown(t *testing.T) {
	t.Helper()
	if n.engine != nil {
		require.NoError(t, n.engine.Stop())
	}
	require.NoError(t, n.appConns.Stop())
}

// testNetwork wires together multiple test nodes using in-memory peers.
type testNetwork struct {
	nodes map[string]*testNode
}

func newTestNetwork() *testNetwork {
	return &testNetwork{nodes: make(map[string]*testNode)}
}

const (
	stepNone      int8 = 0
	stepPropose   int8 = 1
	stepPrevote   int8 = 2
	stepPrecommit int8 = 3
)

type memoryPrivValidator struct {
	mu sync.Mutex
	pk cmttypes.MockPV

	lastHeight       int64
	lastRound        int32
	lastStep         int8
	lastSignBytes    []byte
	lastSignature    []byte
	lastExtSignature []byte
}

func newMemoryPrivValidator(privKey crypto.PrivKey) *memoryPrivValidator {
	return &memoryPrivValidator{
		pk: cmttypes.NewMockPVWithParams(privKey, false, false),
	}
}

func (m *memoryPrivValidator) GetPubKey() (crypto.PubKey, error) {
	return m.pk.GetPubKey()
}

func (m *memoryPrivValidator) SignVote(chainID string, vote *cmtproto.Vote) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	height, round, step := vote.Height, vote.Round, voteToStep(vote)
	sameHRS, err := m.checkHRS(height, round, step)
	if err != nil {
		return err
	}

	signBytes := cmttypes.VoteSignBytes(chainID, vote)
	if sameHRS {
		if !bytes.Equal(signBytes, m.lastSignBytes) {
			return fmt.Errorf("conflicting vote sign bytes at height %d round %d step %d", height, round, step)
		}
		vote.Signature = append([]byte(nil), m.lastSignature...)
		vote.ExtensionSignature = append([]byte(nil), m.lastExtSignature...)
		return nil
	}

	if err := m.pk.SignVote(chainID, vote); err != nil {
		return err
	}

	m.updateSignState(height, round, step, signBytes, vote.Signature, vote.ExtensionSignature)
	return nil
}

func (m *memoryPrivValidator) SignProposal(chainID string, proposal *cmtproto.Proposal) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	height, round := proposal.Height, proposal.Round
	sameHRS, err := m.checkHRS(height, round, stepPropose)
	if err != nil {
		return err
	}

	signBytes := cmttypes.ProposalSignBytes(chainID, proposal)
	if sameHRS {
		if !bytes.Equal(signBytes, m.lastSignBytes) {
			return fmt.Errorf("conflicting proposal sign bytes at height %d round %d", height, round)
		}
		proposal.Signature = append([]byte(nil), m.lastSignature...)
		return nil
	}

	if err := m.pk.SignProposal(chainID, proposal); err != nil {
		return err
	}

	m.updateSignState(height, round, stepPropose, signBytes, proposal.Signature, nil)
	return nil
}

func (m *memoryPrivValidator) checkHRS(height int64, round int32, step int8) (bool, error) {
	if m.lastHeight > height {
		return false, fmt.Errorf("height regression. Got %d, last height %d", height, m.lastHeight)
	}

	if m.lastHeight == height {
		if m.lastRound > round {
			return false, fmt.Errorf("round regression at height %d. Got %d, last round %d", height, round, m.lastRound)
		}
		if m.lastRound == round {
			if m.lastStep > step {
				return false, fmt.Errorf("step regression at height %d round %d. Got %d, last step %d", height, round, step, m.lastStep)
			}
			if m.lastStep == step {
				if m.lastSignBytes != nil {
					if m.lastSignature == nil {
						panic("memoryPrivValidator: signature nil while sign bytes set")
					}
					return true, nil
				}
				return false, fmt.Errorf("no sign bytes stored for height %d round %d step %d", height, round, step)
			}
		}
	}

	return false, nil
}

func (m *memoryPrivValidator) updateSignState(height int64, round int32, step int8, signBytes, sig, extSig []byte) {
	m.lastHeight = height
	m.lastRound = round
	m.lastStep = step
	m.lastSignBytes = append(m.lastSignBytes[:0], signBytes...)
	m.lastSignature = append(m.lastSignature[:0], sig...)
	if extSig != nil {
		m.lastExtSignature = append(m.lastExtSignature[:0], extSig...)
	} else {
		m.lastExtSignature = m.lastExtSignature[:0]
	}
}

func voteToStep(vote *cmtproto.Vote) int8 {
	switch vote.Type {
	case cmtproto.PrevoteType:
		return stepPrevote
	case cmtproto.PrecommitType:
		return stepPrecommit
	default:
		panic(fmt.Sprintf("unknown vote type: %v", vote.Type))
	}
}

func (net *testNetwork) add(node *testNode) {
	net.nodes[node.name] = node
}

func (net *testNetwork) connect(aName, bName string) {
	a := net.nodes[aName]
	b := net.nodes[bName]

	peerAB := newNetworkPeer(b, b.id)
	peerBA := newNetworkPeer(a, a.id)

	peerAB.counterpart = peerBA
	peerBA.counterpart = peerAB

	a.addPeer(peerAB)
	b.addPeer(peerBA)
}

func (net *testNetwork) connectAll() {
	names := make([]string, 0, len(net.nodes))
	for name := range net.nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			net.connect(names[i], names[j])
		}
	}
}

func (net *testNetwork) heights(names ...string) []int64 {
	heights := make([]int64, len(names))
	for i, name := range names {
		heights[i] = net.nodes[name].height()
	}
	return heights
}

func waitForHeights(t *testing.T, nodes []*testNode, target int64, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		allReached := true
		for _, n := range nodes {
			if n.height() < target {
				allReached = false
				break
			}
		}
		if allReached {
			return
		}

		select {
		case <-ticker.C:
		case <-deadline:
			heights := make([]int64, len(nodes))
			for i, n := range nodes {
				heights[i] = n.height()
			}
			t.Fatalf("timeout waiting for heights >= %d, got %v", target, heights)
		}
	}
}

func ensureHeightsAligned(t *testing.T, nodes []*testNode, maxDelta int64) {
	t.Helper()
	var min, max int64
	for i, n := range nodes {
		h := n.height()
		if i == 0 || h < min {
			min = h
		}
		if h > max {
			max = h
		}
	}
	require.LessOrEqual(t, max-min, maxDelta, "node heights diverged: min=%d max=%d", min, max)
}

func waitForHeightsWithin(t *testing.T, nodes []*testNode, target int64, maxDuration time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	waitForHeights(t, nodes, target, maxDuration+2*time.Second)
	elapsed := time.Since(start)
	require.LessOrEqual(t, elapsed, maxDuration, "expected height %d within %s, took %s", target, maxDuration, elapsed)
	return elapsed
}

func waitForCatchUpWithin(t *testing.T, follower, leader *testNode, maxDelta int64, maxDuration time.Duration) {
	t.Helper()
	deadline := time.After(maxDuration)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		if leader.height()-follower.height() <= maxDelta {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline:
			leaderHeight := leader.height()
			followerHeight := follower.height()
			t.Fatalf("timeout waiting for %s to catch up with %s within %s (leader=%d follower=%d)", follower.name, leader.name, maxDuration, leaderHeight, followerHeight)
		}
	}
}

func blockBudget(blocks int64) time.Duration {
	const startup = 3 * time.Second
	const perBlock = 500 * time.Millisecond
	if blocks < 0 {
		blocks = 0
	}
	return startup + time.Duration(blocks)*perBlock
}

func attestorAddresses(t *testing.T, privVals []cmttypes.PrivValidator) [][]byte {
	t.Helper()
	addrs := make([][]byte, len(privVals))
	for i, pv := range privVals {
		pubKey, err := pv.GetPubKey()
		require.NoError(t, err)
		addr := pubKey.Address()
		addrs[i] = append([]byte(nil), addr...)
	}
	return addrs
}

func loadSeenCommit(node *testNode, height int64) *cmttypes.Commit {
	return node.blockStore.LoadSeenCommit(height)
}

func missingAttestorVotes(commit *cmttypes.Commit, attestorAddrs [][]byte) [][]byte {
	missing := make([][]byte, 0)
	for _, addr := range attestorAddrs {
		found := false
		for _, sig := range commit.Signatures {
			if sig.BlockIDFlag == cmttypes.BlockIDFlagCommit && bytes.Equal(sig.ValidatorAddress, addr) {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, append([]byte(nil), addr...))
		}
	}
	return missing
}

func attestorSharedHeights(
	t *testing.T,
	nodes []*testNode,
	attestors []cmttypes.PrivValidator,
	fromHeight int64,
	toHeight int64,
) ([]int64, map[int64]map[string][]string) {
	t.Helper()
	if len(nodes) == 0 || fromHeight > toHeight {
		return nil, nil
	}

	attestorAddrs := attestorAddresses(t, attestors)
	shared := make([]int64, 0, toHeight-fromHeight+1)
	attestorLabels := make(map[string]string, len(attestorAddrs))
	for i, addr := range attestorAddrs {
		hexAddr := fmt.Sprintf("%X", addr)
		attestorLabels[hexAddr] = fmt.Sprintf("attestor-%d", i+1)
	}
	missing := make(map[int64]map[string][]string)

	for height := fromHeight; height <= toHeight; height++ {
		var baseProto *cmtproto.Commit
		allMatch := true
		for i, node := range nodes {
			commit := loadSeenCommit(node, height)
			require.NotNil(t, commit, "node %s missing commit for height %d", node.name, height)
			missingAddrs := missingAttestorVotes(commit, attestorAddrs)
			if len(missingAddrs) > 0 {
				allMatch = false
				formatted := make([]string, len(missingAddrs))
				for idx, addr := range missingAddrs {
					hexAddr := fmt.Sprintf("%X", addr)
					if label, ok := attestorLabels[hexAddr]; ok {
						formatted[idx] = label
						continue
					}
					formatted[idx] = hexAddr
				}
				sort.Strings(formatted)
				nodeMissing := missing[height]
				if nodeMissing == nil {
					nodeMissing = make(map[string][]string)
					missing[height] = nodeMissing
				}
				nodeMissing[node.name] = formatted
			}
			commitProto := commit.ToProto()
			if i == 0 {
				baseProto = commitProto
				continue
			}
			if !gogoproto.Equal(baseProto, commitProto) {
				allMatch = false
			}
		}
		if allMatch {
			shared = append(shared, height)
		}
	}

	return shared, missing
}

func makeSequencingGenesis(t *testing.T) (sm.State, cmttypes.PrivValidator, []cmttypes.PrivValidator) {
	t.Helper()

	seqPrivKey := ed25519.GenPrivKey()
	sequencerPV := newMemoryPrivValidator(seqPrivKey)

	attestors := make([]cmttypes.PrivValidator, 3)
	validators := make([]cmttypes.GenesisValidator, 0, 4)

	seqPubKey, err := sequencerPV.GetPubKey()
	require.NoError(t, err)
	validators = append(validators, cmttypes.GenesisValidator{
		Address: seqPubKey.Address(),
		PubKey:  seqPubKey,
		Power:   cmttypes.SequencerVotingPower,
		Name:    "sequencer",
	})

	for i := 0; i < 3; i++ {
		pv := cmttypes.NewMockPV()
		attestors[i] = pv
		pubKey, _ := pv.GetPubKey()
		validators = append(validators, cmttypes.GenesisValidator{
			Address: pubKey.Address(),
			PubKey:  pubKey,
			Power:   cmttypes.AttestorVotingPower,
			Name:    fmt.Sprintf("attestor-%d", i+1),
		})
	}

	genDoc := &cmttypes.GenesisDoc{
		ChainID:       "sequencing-test",
		Validators:    validators,
		InitialHeight: 1,
	}

	state, err := sm.MakeGenesisState(genDoc)
	require.NoError(t, err)

	return state, sequencerPV, attestors
}

func defaultSequencingConfig() config.SequencingConfig {
	cfg := *config.TestSequencingConfig()
	cfg.CreateEmptyBlocks = true
	cfg.CreateEmptyBlocksInterval = 100 * time.Millisecond
	cfg.BlockInterval = 50 * time.Millisecond
	return cfg
}

func TestSequencingBlockThroughputWithApplyDelay(t *testing.T) {
	t.Parallel()

	baseState, sequencerPV, attestorPVs := makeSequencingGenesis(t)
	cfg := defaultSequencingConfig()

	net := newTestNetwork()

	throughputOpts := []testNodeOption{withAppDelay(200 * time.Millisecond), withCommitTracking()}

	seqPrimary := newTestNode(t, "sequencer-primary", baseState, cfg, sequencerPV, throughputOpts...)
	seqSecondary := newTestNode(t, "sequencer-secondary", baseState, cfg, sequencerPV, throughputOpts...)
	at1 := newTestNode(t, "attestor-1", baseState, cfg, attestorPVs[0], throughputOpts...)
	at2 := newTestNode(t, "attestor-2", baseState, cfg, attestorPVs[1], throughputOpts...)
	at3 := newTestNode(t, "attestor-3", baseState, cfg, attestorPVs[2], throughputOpts...)
	fol1 := newTestNode(t, "follower-1", baseState, cfg, cmttypes.NewMockPV(), throughputOpts...)
	fol2 := newTestNode(t, "follower-2", baseState, cfg, cmttypes.NewMockPV(), throughputOpts...)

	nodes := []*testNode{seqPrimary, seqSecondary, at1, at2, at3, fol1, fol2}
	for _, node := range nodes {
		net.add(node)
		n := node
		t.Cleanup(func() { n.shutdown(t) })
	}
	net.connectAll()

	for _, node := range nodes {
		node.start(t)
	}

	warmupTarget := int64(3)
	waitForHeights(t, nodes, warmupTarget, blockBudget(warmupTarget))
	ensureHeightsAligned(t, nodes, 1)

	startHeight := seqPrimary.height()
	blocksMeasured := int64(5)
	targetHeight := startHeight + blocksMeasured

	start := time.Now()
	waitForHeights(t, nodes, targetHeight, blockBudget(targetHeight))
	elapsed := time.Since(start)
	endHeight := seqPrimary.height()
	produced := endHeight - startHeight
	require.Greater(t, produced, int64(0))
	require.GreaterOrEqual(t, produced, blocksMeasured)
	avgPerBlock := elapsed / time.Duration(produced)

	t.Logf("generated %d blocks in %s (~%s per block)", produced, elapsed, avgPerBlock)
	require.LessOrEqual(t, avgPerBlock, 350*time.Millisecond, "average block time exceeded expected delay tolerance")

	sharedHeights, missing := attestorSharedHeights(t, nodes, attestorPVs, startHeight+1, endHeight)
	sharedCount := len(sharedHeights)
	totalHeights := int(produced)
	missingCount := len(missing)
	t.Logf("attestor commits fully shared across peers for %d/%d blocks", sharedCount, totalHeights)
	if missingCount > 0 {
		heightsWithGaps := make([]int64, 0, missingCount)
		for height := range missing {
			heightsWithGaps = append(heightsWithGaps, height)
		}
		sort.Slice(heightsWithGaps, func(i, j int) bool { return heightsWithGaps[i] < heightsWithGaps[j] })
		for _, height := range heightsWithGaps {
			t.Logf("height %d missing attestors by node: %v", height, missing[height])
		}
	}

	ensureHeightsAligned(t, nodes, 1)
}

func TestSequencingNetworkFailoverAndRecovery(t *testing.T) {
	t.Parallel()

	baseState, sequencerPV, attestorPVs := makeSequencingGenesis(t)
	cfg := defaultSequencingConfig()

	net := newTestNetwork()

	seqPrimary := newTestNode(t, "sequencer-primary", baseState, cfg, sequencerPV)
	seqSecondary := newTestNode(t, "sequencer-secondary", baseState, cfg, sequencerPV)

	at1 := newTestNode(t, "attestor-1", baseState, cfg, attestorPVs[0])
	at2 := newTestNode(t, "attestor-2", baseState, cfg, attestorPVs[1])
	at3 := newTestNode(t, "attestor-3", baseState, cfg, attestorPVs[2])

	fol1 := newTestNode(t, "follower-1", baseState, cfg, cmttypes.NewMockPV())
	fol2 := newTestNode(t, "follower-2", baseState, cfg, cmttypes.NewMockPV())

	nodes := []*testNode{seqPrimary, seqSecondary, at1, at2, at3, fol1, fol2}
	for _, n := range nodes {
		net.add(n)
	}
	net.connectAll()

	// Start primary sequencer, attestors, and followers.
	seqPrimary.start(t)
	at1.start(t)
	at2.start(t)
	at3.start(t)
	fol1.start(t)
	fol2.start(t)

	initialNodes := []*testNode{seqPrimary, at1, at2, at3, fol1, fol2}
	initialTarget := seqPrimary.height() + 6
	waitForHeightsWithin(t, initialNodes, initialTarget, blockBudget(6))
	ensureHeightsAligned(t, initialNodes, 1)

	// Fail over to the secondary sequencer.
	failoverStart := seqPrimary.height()
	seqPrimary.stop(t)
	seqSecondary.restart(t)

	activeNodes := []*testNode{seqSecondary, at1, at2, at3, fol1, fol2}
	waitForHeightsWithin(t, activeNodes, failoverStart, blockBudget(failoverStart))
	waitForHeightsWithin(t, activeNodes, failoverStart+5, blockBudget(5))
	ensureHeightsAligned(t, activeNodes, 1)

	// Bring the primary back and ensure it catches up quickly.
	seqPrimary.restart(t)
	waitForCatchUpWithin(t, seqPrimary, seqSecondary, 1, blockBudget(4))
	ensureHeightsAligned(t, []*testNode{seqPrimary, seqSecondary, at1, at2, at3, fol1, fol2}, 1)

	// Stop both sequencers briefly, then revive the secondary.
	seqPrimary.stop(t)
	seqSecondary.stop(t)
	time.Sleep(200 * time.Millisecond)

	seqSecondary.restart(t)
	recoveryTarget := seqSecondary.height() + 3
	waitForHeightsWithin(t, []*testNode{seqSecondary, at1, at2, at3, fol1, fol2}, recoveryTarget, blockBudget(3))
	ensureHeightsAligned(t, []*testNode{seqSecondary, at1, at2, at3, fol1, fol2}, 1)

	seqPrimary.restart(t)
	waitForCatchUpWithin(t, seqPrimary, seqSecondary, 1, blockBudget(3))
	ensureHeightsAligned(t, []*testNode{seqPrimary, seqSecondary, at1, at2, at3, fol1, fol2}, 1)

	// Drop two attestors and ensure blocks keep flowing, then bring them back.
	at1.stop(t)
	at2.stop(t)

	degradedTarget := seqSecondary.height() + 4
	waitForHeightsWithin(t, []*testNode{seqSecondary, at3, fol1, fol2}, degradedTarget, blockBudget(4))
	ensureHeightsAligned(t, []*testNode{seqSecondary, at3, fol1, fol2}, 1)

	at1.restart(t)
	at2.restart(t)
	waitForCatchUpWithin(t, at1, seqSecondary, 1, blockBudget(4))
	waitForCatchUpWithin(t, at2, seqSecondary, 1, blockBudget(4))
	ensureHeightsAligned(t, []*testNode{seqPrimary, seqSecondary, at1, at2, at3, fol1, fol2}, 1)

	for _, n := range nodes {
		n.shutdown(t)
	}
}
