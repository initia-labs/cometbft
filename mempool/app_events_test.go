package mempool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/abci/example/kvstore"
	abci "github.com/cometbft/cometbft/abci/types"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/p2p"
	memproto "github.com/cometbft/cometbft/proto/tendermint/mempool"
	"github.com/cometbft/cometbft/types"
)

// waitForCondition polls until the condition returns true or timeout.
func waitForCondition(timeout time.Duration, condition func() bool) bool {
	deadline := time.After(timeout)
	for {
		if condition() {
			return true
		}
		select {
		case <-deadline:
			return false
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// reactor event handling

func TestAppEventLoop_EventTxInserted_SetsHasValidTxs(t *testing.T) {
	config := cfg.TestConfig()
	reactors, switches := makeAndConnectReactors(config, 1)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()

	mp := reactors[0].mempool
	require.False(t, mp.HasValidTxs())

	tx := types.Tx("inserted-tx")
	mp.AppEventCh() <- AppMempoolEvent{
		Type:  EventTxInserted,
		TxKey: tx.Key(),
		Tx:    tx,
	}

	ok := waitForCondition(2*time.Second, func() bool {
		return mp.HasValidTxs()
	})
	require.True(t, ok, "HasValidTxs should be true after EventTxInserted")
}

func TestAppEventLoop_EventTxInserted_NotifiesTxsAvailable(t *testing.T) {
	config := cfg.TestConfig()
	reactors, switches := makeAndConnectReactors(config, 1)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()

	mp := reactors[0].mempool
	mp.EnableTxsAvailable()

	// push EventTxInserted
	tx := types.Tx("notify-tx")
	mp.AppEventCh() <- AppMempoolEvent{
		Type:  EventTxInserted,
		TxKey: tx.Key(),
		Tx:    tx,
	}

	select {
	case <-mp.TxsAvailable():
	case <-time.After(2 * time.Second):
		t.Fatal("expected TxsAvailable after EventTxInserted")
	}
}

func TestAppEventLoop_EventTxInserted_AddsToInsertedTxs(t *testing.T) {
	config := cfg.TestConfig()
	reactors, switches := makeAndConnectReactors(config, 1)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()

	tx := types.Tx("regossip-tx")
	txKey := tx.Key()

	reactors[0].mempool.AppEventCh() <- AppMempoolEvent{
		Type:  EventTxInserted,
		TxKey: txKey,
		Tx:    tx,
	}

	ok := waitForCondition(2*time.Second, func() bool {
		reactors[0].insertedTxsMtx.Lock()
		_, exists := reactors[0].insertedTxs[txKey]
		reactors[0].insertedTxsMtx.Unlock()
		return exists
	})
	require.True(t, ok, "tx should be in insertedTxs after EventTxInserted")
}

func TestAppEventLoop_EventTxRemoved_RemovesFromInsertedTxs(t *testing.T) {
	config := cfg.TestConfig()
	reactors, switches := makeAndConnectReactors(config, 1)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()

	tx := types.Tx("remove-me")
	txKey := tx.Key()

	// first we insert, then just remove
	reactors[0].mempool.AppEventCh() <- AppMempoolEvent{
		Type:  EventTxInserted,
		TxKey: txKey,
		Tx:    tx,
	}

	ok := waitForCondition(2*time.Second, func() bool {
		reactors[0].insertedTxsMtx.Lock()
		_, exists := reactors[0].insertedTxs[txKey]
		reactors[0].insertedTxsMtx.Unlock()
		return exists
	})
	require.True(t, ok, "tx should be in insertedTxs")

	// now send EventTxRemoved
	reactors[0].mempool.AppEventCh() <- AppMempoolEvent{
		Type:  EventTxRemoved,
		TxKey: txKey,
	}

	ok = waitForCondition(2*time.Second, func() bool {
		reactors[0].insertedTxsMtx.Lock()
		_, exists := reactors[0].insertedTxs[txKey]
		reactors[0].insertedTxsMtx.Unlock()
		return !exists
	})
	require.True(t, ok, "tx should be removed from insertedTxs after EventTxRemoved")
}

func TestAppEventLoop_EventTxRemoved_RemovesFromKnownTxs(t *testing.T) {
	config := cfg.TestConfig()
	reactors, switches := makeAndConnectReactors(config, 1)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()

	mp := reactors[0].mempool

	// add a  single tx to knownTxs via CheckTx
	tx := types.Tx(kvstore.NewRandomTx(20))
	txKey := tx.Key()
	err := mp.CheckTx(tx, nil, TxInfo{SenderID: UnknownPeerID})
	require.NoError(t, err)

	// wait for it to be in knownTxs
	ok := waitForCondition(2*time.Second, func() bool {
		_, exists := mp.knownTxs.Load(txKey)
		return exists
	})
	require.True(t, ok, "tx should be in knownTxs")

	// send EventTxRemoved
	mp.AppEventCh() <- AppMempoolEvent{
		Type:  EventTxRemoved,
		TxKey: txKey,
	}

	ok = waitForCondition(2*time.Second, func() bool {
		_, exists := mp.knownTxs.Load(txKey)
		return !exists
	})
	require.True(t, ok, "tx should be removed from knownTxs after EventTxRemoved")
}

func TestAppEventLoop_EventTxQueued_GossipsToPeers(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors, switches := makeAndConnectReactors(config, N)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	// submit a transaction via reactor[0], should fire EventTxQueued in the callback
	tx := kvstore.NewRandomTx(20)
	err := reactors[0].mempool.CheckTx(tx, nil, TxInfo{SenderID: UnknownPeerID})
	require.NoError(t, err)

	// wait for reactor[1] to receive the tx
	ok := waitForCondition(5*time.Second, func() bool {
		return reactors[1].mempool.Size() >= 1
	})
	require.True(t, ok, "reactor[1] should have received the tx via gossip")
}

func TestReactorUpdate_CommittedTxsRemovedFromKnownTxs(t *testing.T) {
	config := cfg.TestConfig()
	reactors, switches := makeAndConnectReactors(config, 1)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()

	mp := reactors[0].mempool

	// add txs
	txs := addRandomTxs(t, mp, 5, UnknownPeerID)
	require.Equal(t, 5, mp.Size())

	// update with 3 committed txs
	mp.Lock()
	responses := make([]*abci.ExecTxResult, 3)
	for i := range responses {
		responses[i] = &abci.ExecTxResult{Code: 0}
	}
	err := mp.Update(1, txs[:3], responses, nil, nil)
	mp.Unlock()
	require.NoError(t, err)

	// 3 committed txs removed, 2 remain
	require.Equal(t, 2, mp.Size())

	// committed txs should be in included cache
	for _, tx := range txs[:3] {
		require.True(t, mp.IsIncludedTx(tx))
	}

	// non committed txs should not be in included cache
	for _, tx := range txs[3:] {
		require.False(t, mp.IsIncludedTx(tx))
	}
}

func TestGossipTxToPeers_ExcludesSender(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors, switches := makeAndConnectReactors(config, N)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	// get the p2p id of reactor[1] as seen by reactor[0]
	senderPeerID := reactors[0].Switch.Peers().List()[0].ID()

	// submit txs claiming they came from the only connected peer
	txs := NewRandomTxs(10, 20)
	for _, tx := range txs {
		err := reactors[0].mempool.CheckTx(tx, nil, TxInfo{SenderID: UnknownPeerID, SenderP2PID: senderPeerID})
		if err != nil {
			require.ErrorIs(t, err, ErrTxInCache)
		}
	}

	// wait a bit, then verify reactor[1] got no txs, since it was the sender
	time.Sleep(200 * time.Millisecond)
	assert.Zero(t, reactors[1].mempool.Size(), "reactor[1] should not receive txs that it supposedly sent")
}

func TestRegossipBackoff_Exponential(t *testing.T) {
	for i := 0; i < 8; i++ {
		backoff := min(regossipBaseInterval<<min(i, regossipMaxAttempts), regossipMaxInterval)

		if i < 7 {
			expected := regossipBaseInterval << i
			assert.Equal(t, expected, backoff, "attempt %d should have backoff %v", i, expected)
		} else {
			assert.Equal(t, regossipMaxInterval, backoff, "attempt %d should be capped at regossipMaxInterval", i)
		}
	}
}

func TestRegossipBackoff_OverflowProtection(t *testing.T) {
	for attempts := regossipMaxAttempts; attempts <= regossipMaxAttempts+10; attempts++ {
		backoff := min(regossipBaseInterval<<min(attempts, regossipMaxAttempts), regossipMaxInterval)
		assert.True(t, backoff > 0, "backoff should never be negative (attempts=%d)", attempts)
		assert.Equal(t, regossipMaxInterval, backoff, "backoff should be capped at max (attempts=%d)", attempts)
	}
}

func TestRegossipHeightBackoff_Exponential(t *testing.T) {
	for i := 0; i < 8; i++ {
		heightBackoff := min(regossipHeightInterval<<min(i, regossipMaxAttempts), regossipMaxHeightInterval)

		if i < 6 {
			expected := regossipHeightInterval << i
			assert.Equal(t, expected, heightBackoff, "attempt %d should have height backoff %d", i, expected)
		} else {
			assert.Equal(t, regossipMaxHeightInterval, heightBackoff, "attempt %d should be capped at regossipMaxHeightInterval", i)
		}
	}
}

func TestGossipLoop_TimeBased(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors, switches := makeAndConnectReactors(config, N)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	tx := kvstore.NewRandomTx(20)
	txKey := types.Tx(tx).Key()

	reactors[0].insertedTxsMtx.Lock()
	reactors[0].insertedTxs[txKey] = &regossipEntry{
		tx:               tx,
		lastGossipTime:   time.Now().Add(-regossipBaseInterval - time.Second),
		lastGossipHeight: reactors[0].mempool.Height(),
		attempts:         0,
	}
	reactors[0].insertedTxsMtx.Unlock()

	// wait for reactor[1] to receive the tx via regossip
	ok := waitForCondition(5*time.Second, func() bool {
		return reactors[1].mempool.Size() >= 1
	})
	require.True(t, ok, "reactor[1] should receive tx via time-based regossip")
}

func TestRegossipLoop_HeightBased(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors, switches := makeAndConnectReactors(config, N)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	tx := kvstore.NewRandomTx(20)
	txKey := types.Tx(tx).Key()

	// inject a tx with recent lastGossipTime but old lastGossipHeight
	reactors[0].insertedTxsMtx.Lock()
	reactors[0].insertedTxs[txKey] = &regossipEntry{
		tx:               tx,
		lastGossipTime:   time.Now().Add(time.Hour), // far in the future, time won't trigger
		lastGossipHeight: 0,                         // old height, height condition will trigger
		attempts:         0,
	}
	reactors[0].insertedTxsMtx.Unlock()

	// advance height past the height interval
	mp := reactors[0].mempool
	mp.Lock()
	_ = mp.Update(regossipHeightInterval+1, nil, nil, nil, nil)
	mp.Unlock()

	// wait for reactor[1] to receive the tx via height-based regossip
	ok := waitForCondition(5*time.Second, func() bool {
		return reactors[1].mempool.Size() >= 1
	})
	require.True(t, ok, "reactor[1] should receive tx via height-based regossip")
}

func TestRegossipLoop_CleansCommittedTxs(t *testing.T) {
	config := cfg.TestConfig()
	reactors, switches := makeAndConnectReactors(config, 1)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()

	tx := types.Tx("committed-tx")
	txKey := tx.Key()

	// add tx to insertedTxs
	reactors[0].insertedTxsMtx.Lock()
	reactors[0].insertedTxs[txKey] = &regossipEntry{
		tx:               tx,
		lastGossipTime:   time.Now(),
		lastGossipHeight: 0,
	}
	reactors[0].insertedTxsMtx.Unlock()

	mp := reactors[0].mempool
	mp.Lock()
	results := []*abci.ExecTxResult{{Code: 0}}
	_ = mp.Update(1, types.Txs{tx}, results, nil, nil)
	mp.Unlock()

	require.True(t, mp.IsIncludedTx(tx), "tx should be in included cache")

	ok := waitForCondition(5*time.Second, func() bool {
		reactors[0].insertedTxsMtx.Lock()
		_, exists := reactors[0].insertedTxs[txKey]
		reactors[0].insertedTxsMtx.Unlock()
		return !exists
	})
	require.True(t, ok, "committed tx should be cleaned from insertedTxs by regossipLoop")
}

func TestRegossipLoop_AttemptsIncrement(t *testing.T) {
	config := cfg.TestConfig()
	reactors, switches := makeAndConnectReactors(config, 1)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()

	tx := types.Tx("attempts-test")
	txKey := tx.Key()

	// inject tx with expired backoff so it triggers immediately
	reactors[0].insertedTxsMtx.Lock()
	reactors[0].insertedTxs[txKey] = &regossipEntry{
		tx:               tx,
		lastGossipTime:   time.Now().Add(-regossipBaseInterval - time.Second),
		lastGossipHeight: 0,
		attempts:         0,
	}
	reactors[0].insertedTxsMtx.Unlock()

	ok := waitForCondition(5*time.Second, func() bool {
		reactors[0].insertedTxsMtx.Lock()
		entry, exists := reactors[0].insertedTxs[txKey]
		var attempts int
		if exists {
			attempts = entry.attempts
		}
		reactors[0].insertedTxsMtx.Unlock()
		return exists && attempts >= 1
	})
	require.True(t, ok, "attempts should be incremented after regossip")
}

func TestEventTxInserted_DoesNotGossip(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors, switches := makeAndConnectReactors(config, N)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	tx := types.Tx("inserted-no-gossip")

	// send EventTxInserted directly (not via CheckTx which fires EventTxQueued)
	reactors[0].mempool.AppEventCh() <- AppMempoolEvent{
		Type:  EventTxInserted,
		TxKey: tx.Key(),
		Tx:    tx,
	}

	// wait for the event to be processed
	ok := waitForCondition(2*time.Second, func() bool {
		reactors[0].insertedTxsMtx.Lock()
		_, exists := reactors[0].insertedTxs[tx.Key()]
		reactors[0].insertedTxsMtx.Unlock()
		return exists
	})
	require.True(t, ok, "tx should be in insertedTxs")

	time.Sleep(500 * time.Millisecond)
	assert.Zero(t, reactors[1].mempool.Size(),
		"reactor[1] should not receive tx from EventTxInserted (only EventTxQueued gossips)")
}

func TestReactorReceive_EmptyTxs(t *testing.T) {
	config := cfg.TestConfig()
	reactors, switches := makeAndConnectReactors(config, 2)
	defer func() {
		for _, s := range switches {
			_ = s.Stop()
		}
	}()

	// receiving empty txs should not panic
	peer := reactors[0].Switch.Peers().List()[0]
	require.NotPanics(t, func() {
		reactors[0].Receive(p2p.Envelope{
			ChannelID: MempoolChannel,
			Src:       peer,
			Message:   &memproto.Txs{Txs: [][]byte{}},
		})
	})
}
