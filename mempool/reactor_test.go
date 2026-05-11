package mempool

import (
	"encoding/hex"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/log/term"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/abci/example/kvstore"
	abci "github.com/cometbft/cometbft/abci/types"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/internal/test"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/mock"
	memproto "github.com/cometbft/cometbft/proto/tendermint/mempool"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
)

const (
	numTxs  = 1000
	timeout = 120 * time.Second // ridiculously high because CircleCI is slow
)

type peerState struct {
	height int64
}

func (ps peerState) GetHeight() int64 {
	return ps.height
}

// Send a bunch of txs to the first reactor's mempool and wait for them all to
// be received in the others.
func TestReactorBroadcastTxsMessage(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	txs := addRandomTxs(t, reactors[0].mempool, numTxs, UnknownPeerID)
	waitForTxsOnReactors(t, txs, reactors)
}

// regression test for https://github.com/cometbft/cometbft/issues/5408
func TestReactorConcurrency(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}
	var wg sync.WaitGroup

	const numTxs = 5

	for i := 0; i < 1000; i++ {
		wg.Add(2)

		// 1. submit a bunch of txs
		// 2. update the whole mempool
		txs := addRandomTxs(t, reactors[0].mempool, numTxs, UnknownPeerID)
		go func() {
			defer wg.Done()

			reactors[0].mempool.Lock()
			defer reactors[0].mempool.Unlock()

			txResponses := make([]*abci.ExecTxResult, len(txs))
			for i := range txs {
				txResponses[i] = &abci.ExecTxResult{Code: 0}
			}
			err := reactors[0].mempool.Update(1, txs, txResponses, nil, nil)
			assert.NoError(t, err)
		}()

		// 1. submit a bunch of txs
		// 2. update none
		_ = addRandomTxs(t, reactors[1].mempool, numTxs, UnknownPeerID)
		go func() {
			defer wg.Done()

			reactors[1].mempool.Lock()
			defer reactors[1].mempool.Unlock()
			err := reactors[1].mempool.Update(1, []types.Tx{}, make([]*abci.ExecTxResult, 0), nil, nil)
			assert.NoError(t, err)
		}()

		// 1. flush the mempool
		reactors[1].mempool.Flush()
	}

	wg.Wait()
}

// Send a bunch of txs to the first reactor's mempool, claiming it came from the
// only connected peer. Ensure that the peer gets no txs back.
func TestReactorNoBroadcastToSender(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	// get the p2p.ID of reactor[1] as seen by reactor[0].
	senderPeerID := reactors[0].Switch.Peers().List()[0].ID()

	txs := NewRandomTxs(numTxs, 20)
	txInfo := TxInfo{SenderID: UnknownPeerID, SenderP2PID: senderPeerID}
	for _, tx := range txs {
		err := reactors[0].mempool.CheckTx(tx, nil, txInfo)
		if err != nil && !errors.Is(err, ErrTxInCache) {
			t.Fatalf("CheckTx failed: %v", err)
		}
	}

	ensureNoTxs(t, reactors[1], 100*time.Millisecond)
}

func TestMempoolReactorMaxTxBytes(t *testing.T) {
	config := cfg.TestConfig()

	const N = 2
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	// Broadcast a tx, which has the max size
	// => ensure it's received by the second reactor.
	tx1 := kvstore.NewRandomTx(config.Mempool.MaxTxBytes)
	err := reactors[0].mempool.CheckTx(tx1, func(resp *abci.ResponseCheckTx) {
		require.False(t, resp.IsErr())
	}, TxInfo{SenderID: UnknownPeerID})
	require.NoError(t, err)
	waitForTxsOnReactors(t, []types.Tx{tx1}, reactors)

	reactors[0].mempool.Flush()
	reactors[1].mempool.Flush()

	// Broadcast a tx, which is beyond the max size
	// => ensure it's not sent
	tx2 := kvstore.NewRandomTx(config.Mempool.MaxTxBytes + 1)
	err = reactors[0].mempool.CheckTx(tx2, func(resp *abci.ResponseCheckTx) {
		require.False(t, resp.IsErr())
	}, TxInfo{SenderID: UnknownPeerID})
	require.Error(t, err)
}

func TestBroadcastTxForPeerStopsWhenPeerStops(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	config := cfg.TestConfig()
	const N = 2
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()

	// stop peer
	sw := reactors[1].Switch
	sw.StopPeerForError(sw.Peers().List()[0], errors.New("some reason"))

	// check that we are not leaking any go-routines
	// i.e. checkTxRoutine finishes when the peer is stopped
	leaktest.CheckTimeout(t, 10*time.Second)()
}

func TestBroadcastTxForPeerStopsWhenReactorStops(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	config := cfg.TestConfig()
	const N = 2
	_, switches := makeAndConnectReactors(config, N)

	// stop reactors
	for _, s := range switches {
		assert.NoError(t, s.Stop())
	}

	// check that we are not leaking any go-routines
	// i.e. checkTxRoutine finishes when the reactor is stopped
	leaktest.CheckTimeout(t, 10*time.Second)()
}

// TODO: This test tests that we don't panic and are able to generate new
// PeerIDs for each peer we add. It seems as though we should be able to test
// this in a much more direct way.
// https://github.com/cometbft/cometbft/issues/9639
func TestDontExhaustMaxActiveIDs(t *testing.T) {
	config := cfg.TestConfig()
	const N = 1
	reactors, _ := makeAndConnectReactors(config, N)
	defer func() {
		for _, r := range reactors {
			if err := r.Stop(); err != nil {
				assert.NoError(t, err)
			}
		}
	}()
	reactor := reactors[0]

	for i := 0; i < MaxActiveIDs+1; i++ {
		peer := mock.NewPeer(nil)
		reactor.Receive(p2p.Envelope{
			ChannelID: MempoolChannel,
			Src:       peer,
			Message:   &memproto.Message{}, // This uses the wrong message type on purpose to stop the peer as in an error state in the reactor.
		},
		)
		reactor.AddPeer(peer)
	}
}

// mempoolLogger is a TestingLogger which uses a different
// color for each validator ("validator" key must exist).
func mempoolLogger() log.Logger {
	return log.TestingLoggerWithColorFn(func(keyvals ...interface{}) term.FgBgColor {
		for i := 0; i < len(keyvals)-1; i += 2 {
			if keyvals[i] == "validator" {
				return term.FgBgColor{Fg: term.Color(uint8(keyvals[i+1].(int) + 1))}
			}
		}
		return term.FgBgColor{}
	})
}

// connect N mempool reactors through N switches
func makeAndConnectReactors(config *cfg.Config, n int) ([]*Reactor, []*p2p.Switch) {
	reactors := make([]*Reactor, n)
	logger := mempoolLogger()
	for i := 0; i < n; i++ {
		app := kvstore.NewInMemoryApplication()
		cc := proxy.NewLocalClientCreator(app)
		mempool, cleanup := newProxyMempoolWithApp(cc)
		defer cleanup()

		reactors[i] = NewReactor(config.Mempool, mempool)
		reactors[i].SetLogger(logger.With("validator", i))
	}

	switches := p2p.MakeConnectedSwitches(config.P2P, n, func(i int, s *p2p.Switch) *p2p.Switch {
		s.AddReactor("MEMPOOL", reactors[i])
		return s

	}, p2p.Connect2Switches)
	return reactors, switches
}

func newProxyMempoolWithApp(cc proxy.ClientCreator) (*ProxyMempool, func()) {
	conf := test.ResetTestRoot("mempool_test")

	appConnMem, _ := cc.NewABCIClient()
	appConnMem.SetLogger(log.TestingLogger().With("module", "abci-client", "connection", "mempool"))
	err := appConnMem.Start()
	if err != nil {
		panic(err)
	}

	mp := NewProxyMempool(conf.Mempool, appConnMem, 0)
	mp.SetLogger(log.TestingLogger())

	return mp, func() { os.RemoveAll(conf.RootDir) }
}

func newUniqueTxs(n int) types.Txs {
	txs := make(types.Txs, n)
	for i := 0; i < n; i++ {
		txs[i] = kvstore.NewTxFromID(i)
	}
	return txs
}

func waitForTxsOnReactors(t *testing.T, txs types.Txs, reactors []*Reactor) {
	// wait for the txs in all mempools
	wg := new(sync.WaitGroup)
	for i, reactor := range reactors {
		wg.Add(1)
		go func(r *Reactor, reactorIndex int) {
			defer wg.Done()
			waitForNumTxsInMempool(len(txs), r.mempool)
		}(reactor, i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.After(timeout)
	select {
	case <-timer:
		t.Fatal("Timed out waiting for txs")
	case <-done:
	}
}

// Wait until the mempool has a certain number of transactions.
func waitForNumTxsInMempool(numTxs int, mempool Mempool) {
	for mempool.Size() < numTxs {
		time.Sleep(time.Millisecond * 100)
	}
}

// ensure no txs on reactor after some timeout
func ensureNoTxs(t *testing.T, reactor *Reactor, timeout time.Duration) {
	time.Sleep(timeout) // wait for the txs in all mempools
	assert.Zero(t, reactor.mempool.Size())
}

func TestMempoolVectors(t *testing.T) {
	testCases := []struct {
		testName string
		tx       []byte
		expBytes string
	}{
		{"tx 1", []byte{123}, "0a030a017b"},
		{"tx 2", []byte("proto encoding in mempool"), "0a1b0a1970726f746f20656e636f64696e6720696e206d656d706f6f6c"},
	}

	for _, tc := range testCases {

		msg := memproto.Message{
			Sum: &memproto.Message_Txs{
				Txs: &memproto.Txs{Txs: [][]byte{tc.tx}},
			},
		}
		bz, err := msg.Marshal()
		require.NoError(t, err, tc.testName)

		require.Equal(t, tc.expBytes, hex.EncodeToString(bz), tc.testName)
	}
}
