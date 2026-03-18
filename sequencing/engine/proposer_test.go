package engine

import (
	"sync"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/proxy"
	seqtypes "github.com/cometbft/cometbft/sequencing/types"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
	"github.com/stretchr/testify/require"
)

// stubMempool is a minimal mempool implementation for proposer-focused tests.
type stubMempool struct {
	mu     sync.Mutex
	lockMu sync.Mutex
	txs    cmttypes.Txs
}

func newStubMempool() *stubMempool {
	return &stubMempool{}
}

func (m *stubMempool) setTxs(txs cmttypes.Txs) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txs = append(cmttypes.Txs(nil), txs...)
}

func (m *stubMempool) CheckTx(cmttypes.Tx, func(*abci.ResponseCheckTx), mempool.TxInfo) error {
	return nil
}

func (m *stubMempool) RemoveTxByKey(cmttypes.TxKey) error {
	return nil
}

func (m *stubMempool) ReapMaxBytesMaxGas(int64, int64) cmttypes.Txs {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.txs) == 0 {
		return nil
	}
	txs := make(cmttypes.Txs, len(m.txs))
	copy(txs, m.txs)
	return txs
}

func (m *stubMempool) ReapMaxTxs(max int) cmttypes.Txs {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.txs) == 0 {
		return nil
	}
	limit := len(m.txs)
	if max >= 0 && limit > max {
		limit = max
	}
	txs := make(cmttypes.Txs, limit)
	copy(txs, m.txs[:limit])
	return txs
}

func (m *stubMempool) Lock() {
	m.lockMu.Lock()
}

func (m *stubMempool) Unlock() {
	m.lockMu.Unlock()
}

func (m *stubMempool) Update(int64, cmttypes.Txs, []*abci.ExecTxResult, mempool.PreCheckFunc, mempool.PostCheckFunc) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txs = nil
	return nil
}

func (m *stubMempool) FlushAppConn() error {
	return nil
}

func (m *stubMempool) Flush() {
	m.mu.Lock()
	m.txs = nil
	m.mu.Unlock()
}

func (m *stubMempool) TxsAvailable() <-chan struct{} {
	return nil
}

func (m *stubMempool) EnableTxsAvailable() {}

func (m *stubMempool) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.txs)
}

func (m *stubMempool) SizeBytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total int64
	for _, tx := range m.txs {
		total += int64(len(tx))
	}
	return total
}

// stubReactor lets tests control reactor signals.
type stubReactor struct {
	mu          sync.RWMutex
	mempoolSize int
	txsCh       chan struct{}
}

func newStubReactor() *stubReactor {
	return &stubReactor{txsCh: make(chan struct{}, 1)}
}

func (r *stubReactor) Peers() []p2p.Peer { return nil }

func (r *stubReactor) SelfID() p2p.ID { return seqtypes.SELF_PEER_ID }

func (r *stubReactor) PeerIDs() []p2p.ID { return nil }

func (r *stubReactor) Peer(p2p.ID) p2p.Peer { return nil }

func (r *stubReactor) MempoolSize() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mempoolSize
}

func (r *stubReactor) TxsAvailable() <-chan struct{} {
	return r.txsCh
}

func (r *stubReactor) setMempoolSize(size int) {
	r.mu.Lock()
	r.mempoolSize = size
	r.mu.Unlock()
}

func (r *stubReactor) notifyTxsAvailable() {
	select {
	case r.txsCh <- struct{}{}:
	default:
	}
}

func (r *stubReactor) ReportConflictingVotes(vote1, vote2 *cmttypes.Vote) {}

func newProposerTestEngine(t *testing.T, interval time.Duration) (*Engine, *stubReactor, *stubMempool) {
	t.Helper()

	baseState, pv, _ := makeSequencingGenesis(t)
	stateCopy := baseState.Copy()
	statePtr := &stateCopy

	stateDB := dbm.NewMemDB()
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{DiscardABCIResponses: false})
	require.NoError(t, stateStore.Save(stateCopy))

	blockStore := store.NewBlockStore(dbm.NewMemDB())

	app := &testApp{}
	appConns := proxy.NewAppConns(proxy.NewLocalClientCreator(app), proxy.NopMetrics())
	require.NoError(t, appConns.Start())
	t.Cleanup(func() {
		require.NoError(t, appConns.Stop())
	})

	mempool := newStubMempool()

	blockExec := sm.NewBlockExecutor(
		stateStore,
		log.NewNopLogger(),
		appConns.Consensus(),
		mempool,
		sm.EmptyEvidencePool{},
		blockStore,
	)

	reactor := newStubReactor()
	cfg := *config.TestSequencingConfig()
	cfg.CreateEmptyBlocks = false
	cfg.CreateEmptyBlocksInterval = interval
	cfg.BlockInterval = 10 * time.Millisecond

	eng := NewEngine(log.NewNopLogger(), reactor, cfg, statePtr, pv, blockExec, blockStore, nil)

	return eng, reactor, mempool
}

func produceAndApplyBlock(t *testing.T, eng *Engine) *seqtypes.ProposedBlock {
	t.Helper()

	eng.proposeBlock()

	_, height, proposed, ok := eng.blockBucket.PopLowest()
	require.True(t, ok)
	require.Equal(t, proposed.Block.Height, height)

	bad, applied, upgrade := eng.applyProposedBlock(proposed)
	require.False(t, bad)
	require.True(t, applied)
	require.False(t, upgrade)

	require.NoError(t, eng.blockStore.SaveSeenCommit(height, proposed.Commit.ToCommit()))

	return proposed
}

func TestProposeBlockRespectsCreateEmptyInterval(t *testing.T) {
	eng, reactor, _ := newProposerTestEngine(t, 500*time.Millisecond)

	produceAndApplyBlock(t, eng)

	reactor.setMempoolSize(0)

	eng.stateMu.Lock()
	eng.lastProposedBlockTime = cmttime.Now().Add(-eng.cfg.BlockInterval).Add(-time.Millisecond)
	eng.lastProposedBlockNumTxs = 0
	eng.stateMu.Unlock()

	eng.proposeBlock()

	require.Equal(t, 0, eng.blockBucket.Len())

	eng.stateMu.Lock()
	eng.lastProposedBlockTime = cmttime.Now().Add(-eng.cfg.CreateEmptyBlocksInterval).Add(-time.Millisecond)
	eng.stateMu.Unlock()

	eng.proposeBlock()

	require.Equal(t, 1, eng.blockBucket.Len())
}

func TestProposerProcessorCreatesBlockOnTxsAvailable(t *testing.T) {
	eng, reactor, mempool := newProposerTestEngine(t, 500*time.Millisecond)

	produceAndApplyBlock(t, eng)

	tx := cmttypes.Tx("tx-1")
	mempool.setTxs(cmttypes.Txs{tx})
	reactor.setMempoolSize(1)

	eng.stateMu.Lock()
	eng.lastProposedBlockTime = cmttime.Now().Add(-eng.cfg.BlockInterval).Add(-time.Millisecond)
	eng.stateMu.Unlock()

	done := make(chan struct{})
	go func() {
		eng.proposerProcessor()
		close(done)
	}()
	defer func() {
		require.NoError(t, eng.Stop())
		<-done
	}()

	require.Eventually(t, func() bool {
		reactor.notifyTxsAvailable()
		time.Sleep(10 * time.Millisecond)
		return eng.blockBucket.Len() > 0
	}, 300*time.Millisecond, 20*time.Millisecond)
}

// resetForRestart mimics a fresh engine start: clears the tracking fields
// that would be zero-valued on a real restart while keeping the block store.
func resetForRestart(eng *Engine) {
	eng.stateMu.Lock()
	eng.lastProposedBlockHeight = 0
	eng.lastProposedBlockTime = time.Time{}
	eng.lastProposedBlockNumTxs = 0
	eng.stateMu.Unlock()
}

// TestProposeBlockSavesPendingProposal verifies that proposeBlock persists
// a signed pending proposal to the block store.
func TestProposeBlockSavesPendingProposal(t *testing.T) {
	eng, _, _ := newProposerTestEngine(t, 500*time.Millisecond)

	b, c := eng.blockStore.LoadPendingProposal()
	require.Nil(t, b)
	require.Nil(t, c)

	eng.proposeBlock()

	b, c = eng.blockStore.LoadPendingProposal()
	require.NotNil(t, b, "block should be persisted after proposeBlock")
	require.NotNil(t, c, "signed commit should be persisted after proposeBlock")
	require.Equal(t, int64(1), b.Height)
}

// TestProposeBlockDeletesPendingProposalOnApply verifies that
// applyProposedBlock removes the pending proposal once the block is durable.
func TestProposeBlockDeletesPendingProposalOnApply(t *testing.T) {
	eng, _, _ := newProposerTestEngine(t, 500*time.Millisecond)

	eng.proposeBlock()

	b, _ := eng.blockStore.LoadPendingProposal()
	require.NotNil(t, b, "pending proposal should exist before apply")

	_, _, proposed, ok := eng.blockBucket.PopLowest()
	require.True(t, ok)

	bad, applied, _ := eng.applyProposedBlock(proposed)
	require.False(t, bad)
	require.True(t, applied)

	b, _ = eng.blockStore.LoadPendingProposal()
	require.Nil(t, b, "pending proposal should be deleted after apply")
}

// TestProposeBlockRecoversSigned verifies that when a signed pending proposal
// exists for the current height on restart, proposeBlock reuses it without
// re-creating or re-signing the block.
func TestProposeBlockRecoversSigned(t *testing.T) {
	eng, _, _ := newProposerTestEngine(t, 500*time.Millisecond)

	eng.proposeBlock()

	_, _, firstProposed, ok := eng.blockBucket.PopLowest()
	require.True(t, ok)
	originalHash := firstProposed.Block.Hash()

	// Simulate restart: pending proposal is still in the block store,
	// but the in-memory tracking fields are reset to zero values.
	resetForRestart(eng)

	eng.proposeBlock()

	_, _, recovered, ok := eng.blockBucket.PopLowest()
	require.True(t, ok, "block should be produced via recovery")
	require.Equal(t, originalHash, recovered.Block.Hash(), "recovered block must have the same content")
	require.Equal(t, firstProposed.Commit.Height, recovered.Commit.Height)
}

// TestProposeBlockRecoversUnsigned verifies that when only an unsigned
// (pre-sign) pending proposal exists, proposeBlock reuses the block and
// signs it again rather than creating a new block.
func TestProposeBlockRecoversUnsigned(t *testing.T) {
	eng, _, _ := newProposerTestEngine(t, 500*time.Millisecond)

	eng.proposeBlock()

	_, _, firstProposed, ok := eng.blockBucket.PopLowest()
	require.True(t, ok)
	originalHash := firstProposed.Block.Hash()

	// Overwrite with the unsigned (pre-sign) state to simulate a crash
	// that occurred after block creation but before signing.
	eng.blockStore.SavePendingProposal(firstProposed.Block, nil)

	resetForRestart(eng)

	eng.proposeBlock()

	_, _, recovered, ok := eng.blockBucket.PopLowest()
	require.True(t, ok, "block should be produced via recovery")
	require.Equal(t, originalHash, recovered.Block.Hash(), "recovered block must have the same content")
	require.NotNil(t, recovered.Commit, "recovered block must be signed")
}
