package mempool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	abcicli "github.com/cometbft/cometbft/abci/client"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/proxy/mocks"
	"github.com/cometbft/cometbft/types"
)

func newTestConfig() *config.MempoolConfig {
	return &config.MempoolConfig{
		MaxTxBytes: 1024,
		CacheSize:  100,
		Broadcast:  true,
	}
}

// newMockAppConn returns a mock AppConnMempool that accepts SetResponseCallback.
func newMockAppConn(t *testing.T) *mocks.AppConnMempool {
	t.Helper()
	conn := mocks.NewAppConnMempool(t)
	conn.On("SetResponseCallback", mock.Anything).Return()
	return conn
}

// makeReqRes creates a ReqRes with the response already set and callbackInvoked=true
// so that SetCallback immediately invokes the callback.
func makeReqRes(req *abci.RequestCheckTx, code uint32) *abcicli.ReqRes {
	reqRes := abcicli.NewReqRes(&abci.Request{
		Value: &abci.Request_CheckTx{CheckTx: req},
	})

	reqRes.Response = &abci.Response{
		Value: &abci.Response_CheckTx{CheckTx: &abci.ResponseCheckTx{Code: code}},
	}

	reqRes.InvokeCallback()

	return reqRes
}

// setupCheckTxAsyncOK sets up the mock to return a ReqRes that immediately
// invokes the callback with a CodeTypeOK CheckTx response.
func setupCheckTxAsyncOK(conn *mocks.AppConnMempool) {
	conn.On("CheckTxAsync", mock.Anything, mock.Anything).Return(
		func(_ context.Context, req *abci.RequestCheckTx) *abcicli.ReqRes {
			return makeReqRes(req, abci.CodeTypeOK)
		},
		func(_ context.Context, _ *abci.RequestCheckTx) error {
			return nil
		},
	)
}

// setupCheckTxAsyncFail sets up the mock to return a ReqRes that invokes
// the callback with a non-zero code (failure).
func setupCheckTxAsyncFail(conn *mocks.AppConnMempool, code uint32) {
	conn.On("CheckTxAsync", mock.Anything, mock.Anything).Return(
		func(_ context.Context, req *abci.RequestCheckTx) *abcicli.ReqRes {
			return makeReqRes(req, code)
		},
		func(_ context.Context, _ *abci.RequestCheckTx) error {
			return nil
		},
	)
}

// setupCheckTxAsyncError sets up the mock to return an error from CheckTxAsync.
func setupCheckTxAsyncError(conn *mocks.AppConnMempool, err error) {
	conn.On("CheckTxAsync", mock.Anything, mock.Anything).Return(
		(*abcicli.ReqRes)(nil),
		err,
	)
}

func TestProxyMempool_CheckTx_SizeValidation(t *testing.T) {
	conn := newMockAppConn(t)
	cfg := newTestConfig()
	cfg.MaxTxBytes = 10

	mp := NewProxyMempool(cfg, conn, 0)
	mp.SetLogger(log.NewNopLogger())

	t.Run("tx within size limit proceeds", func(t *testing.T) {
		setupCheckTxAsyncOK(conn)
		err := mp.CheckTx(types.Tx("small"), nil, TxInfo{})
		require.NoError(t, err)
	})

	t.Run("tx exceeding MaxTxBytes returns ErrTxTooLarge", func(t *testing.T) {
		bigTx := make(types.Tx, 11)
		err := mp.CheckTx(bigTx, nil, TxInfo{})
		require.Error(t, err)
		var errTooLarge ErrTxTooLarge
		require.True(t, errors.As(err, &errTooLarge))
		require.Equal(t, 10, errTooLarge.Max)
		require.Equal(t, 11, errTooLarge.Actual)
	})
}

func TestProxyMempool_CheckTx_Dedup(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncOK(conn)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	tx := types.Tx("hello")
	err := mp.CheckTx(tx, nil, TxInfo{})
	require.NoError(t, err)

	drainAppEventCh(mp)

	t.Run("same tx returns ErrTxInCache", func(t *testing.T) {
		err := mp.CheckTx(tx, nil, TxInfo{})
		require.ErrorIs(t, err, ErrTxInCache)
	})

	t.Run("different tx passes", func(t *testing.T) {
		err := mp.CheckTx(types.Tx("world"), nil, TxInfo{})
		require.NoError(t, err)
	})
}

func TestProxyMempool_CheckTx_IncludedTxCache(t *testing.T) {
	conn := newMockAppConn(t)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	tx := types.Tx("committed-tx")

	// simulate tx was committed in a block
	mp.includedTxCache.Push(tx)

	err := mp.CheckTx(tx, nil, TxInfo{})
	require.ErrorIs(t, err, ErrTxInCache)
}

func TestProxyMempool_CheckTx_EventTxQueued(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncOK(conn)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	tx := types.Tx("event-tx")
	err := mp.CheckTx(tx, nil, TxInfo{SenderP2PID: "peerX"})
	require.NoError(t, err)

	select {
	case ev := <-mp.AppEventCh():
		require.Equal(t, EventTxQueued, ev.Type)
		require.Equal(t, tx.Key(), ev.TxKey)
		require.Equal(t, types.Tx("event-tx"), ev.Tx)
		require.Equal(t, "peerX", string(ev.SenderID))
	case <-time.After(time.Second):
		t.Fatal("expected EventTxQueued but timed out")
	}
}

func TestProxyMempool_CheckTx_Callback(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncOK(conn)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	var received *abci.ResponseCheckTx
	err := mp.CheckTx(types.Tx("cb-tx"), func(res *abci.ResponseCheckTx) {
		received = res
	}, TxInfo{})
	require.NoError(t, err)
	drainAppEventCh(mp)

	require.NotNil(t, received)
	require.Equal(t, abci.CodeTypeOK, received.Code)
}

func TestProxyMempool_CheckTx_FailedCheckTx(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncFail(conn, 1) // non-zero code

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	tx := types.Tx("bad-tx")
	err := mp.CheckTx(tx, nil, TxInfo{})
	require.NoError(t, err)

	select {
	case ev := <-mp.AppEventCh():
		t.Fatalf("unexpected event: %v", ev)
	case <-time.After(50 * time.Millisecond):
	}

	// tx should not be in knownTxs
	require.Equal(t, 0, mp.Size())
}

func TestProxyMempool_CheckTx_AppConnError(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncError(conn, fmt.Errorf("connection failed"))

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	err := mp.CheckTx(types.Tx("err-tx"), nil, TxInfo{})
	require.Error(t, err)
	var errAppConn ErrAppConnMempool
	require.True(t, errors.As(err, &errAppConn))
}

func TestProxyMempool_RemoveTxByKey(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncOK(conn)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	tx := types.Tx("removable-tx")
	require.NoError(t, mp.CheckTx(tx, nil, TxInfo{}))
	drainAppEventCh(mp)

	require.Equal(t, 1, mp.Size())
	require.Equal(t, int64(len(tx)), mp.SizeBytes())

	t.Run("remove existing tx", func(t *testing.T) {
		err := mp.RemoveTxByKey(tx.Key())
		require.NoError(t, err)
		require.Equal(t, 0, mp.Size())
		require.Equal(t, int64(0), mp.SizeBytes())
	})

	t.Run("remove non-existent tx returns ErrTxNotFound", func(t *testing.T) {
		err := mp.RemoveTxByKey(types.Tx("nonexistent").Key())
		require.ErrorIs(t, err, ErrTxNotFound)
	})
}

func TestProxyMempool_Update(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncOK(conn)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	txs := make([]types.Tx, 3)
	for i := 0; i < 3; i++ {
		txs[i] = types.Tx(fmt.Sprintf("tx-%d", i))
		require.NoError(t, mp.CheckTx(txs[i], nil, TxInfo{}))
		drainAppEventCh(mp)
	}
	require.Equal(t, 3, mp.Size())

	mp.notifiedTxsAvailable.Store(true)
	mp.hasValidTxs.Store(true)

	txResults := make([]*abci.ExecTxResult, 2)
	for i := range txResults {
		txResults[i] = &abci.ExecTxResult{Code: 0}
	}

	t.Run("height is updated", func(t *testing.T) {
		err := mp.Update(10, txs[:2], txResults, nil, nil)
		require.NoError(t, err)
		require.Equal(t, int64(10), mp.Height())
	})

	t.Run("committed txs removed from knownTxs", func(t *testing.T) {
		require.Equal(t, 1, mp.Size()) // only txs[2] remains
	})

	t.Run("committed txs added to includedTxCache", func(t *testing.T) {
		require.True(t, mp.IsIncludedTx(txs[0]))
		require.True(t, mp.IsIncludedTx(txs[1]))
		require.False(t, mp.IsIncludedTx(txs[2]))
	})

	t.Run("notifiedTxsAvailable is reset", func(t *testing.T) {
		require.False(t, mp.notifiedTxsAvailable.Load())
	})

	t.Run("hasValidTxs triggers renotification", func(t *testing.T) {
		require.True(t, mp.hasValidTxs.Load())
	})
}

func TestProxyMempool_Flush(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncOK(conn)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	for i := 0; i < 5; i++ {
		require.NoError(t, mp.CheckTx(types.Tx(fmt.Sprintf("flush-%d", i)), nil, TxInfo{}))
		drainAppEventCh(mp)
	}
	require.Equal(t, 5, mp.Size())

	mp.Flush()

	require.Equal(t, 0, mp.Size())
	require.Equal(t, int64(0), mp.SizeBytes())
}

func TestProxyMempool_FlushAppConn(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		conn := newMockAppConn(t)
		conn.On("Flush", mock.Anything).Return(nil)

		mp := NewProxyMempool(newTestConfig(), conn, 0)
		mp.SetLogger(log.NewNopLogger())

		err := mp.FlushAppConn()
		require.NoError(t, err)
	})

	t.Run("error wraps as ErrFlushAppConn", func(t *testing.T) {
		conn := newMockAppConn(t)
		conn.On("Flush", mock.Anything).Return(fmt.Errorf("flush failed"))

		mp := NewProxyMempool(newTestConfig(), conn, 0)
		mp.SetLogger(log.NewNopLogger())

		err := mp.FlushAppConn()
		require.Error(t, err)
		var errFlush ErrFlushAppConn
		require.True(t, errors.As(err, &errFlush))
	})
}

func TestProxyMempool_SizeAndSizeBytes(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncOK(conn)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	require.Equal(t, 0, mp.Size())
	require.Equal(t, int64(0), mp.SizeBytes())

	tx1 := types.Tx("aaaa")
	tx2 := types.Tx("bbbbbb")
	require.NoError(t, mp.CheckTx(tx1, nil, TxInfo{}))
	drainAppEventCh(mp)
	require.NoError(t, mp.CheckTx(tx2, nil, TxInfo{}))
	drainAppEventCh(mp)

	require.Equal(t, 2, mp.Size())
	require.Equal(t, int64(10), mp.SizeBytes())

	mp.RemoveTxByKey(tx1.Key())
	require.Equal(t, 1, mp.Size())
	require.Equal(t, int64(6), mp.SizeBytes())
}

func TestProxyMempool_HasValidTxs(t *testing.T) {
	conn := newMockAppConn(t)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	require.False(t, mp.HasValidTxs())

	mp.SetHasValidTxs(true)
	require.True(t, mp.HasValidTxs())

	mp.SetHasValidTxs(false)
	require.False(t, mp.HasValidTxs())
}

func TestProxyMempool_TxsAvailable(t *testing.T) {
	conn := newMockAppConn(t)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	t.Run("nil before EnableTxsAvailable", func(t *testing.T) {
		require.Nil(t, mp.TxsAvailable())
	})

	t.Run("non-nil after EnableTxsAvailable", func(t *testing.T) {
		mp.EnableTxsAvailable()
		require.NotNil(t, mp.TxsAvailable())
	})
}

func TestProxyMempool_NotifyTxsAvailable(t *testing.T) {
	conn := newMockAppConn(t)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())
	mp.EnableTxsAvailable()

	t.Run("fires once per height", func(t *testing.T) {
		mp.NotifyTxsAvailable()

		select {
		case <-mp.TxsAvailable():
		case <-time.After(time.Second):
			t.Fatal("expected TxsAvailable signal")
		}

		mp.NotifyTxsAvailable()
		select {
		case <-mp.TxsAvailable():
			t.Fatal("should not fire twice")
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("resets after Update", func(t *testing.T) {
		mp.Update(1, nil, nil, nil, nil)

		mp.NotifyTxsAvailable()
		select {
		case <-mp.TxsAvailable():
			// good
		case <-time.After(time.Second):
			t.Fatal("expected TxsAvailable signal after Update reset")
		}
	})
}

func TestProxyMempool_NotifyTxsAvailable_NilChannel(t *testing.T) {
	conn := newMockAppConn(t)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	require.NotPanics(t, func() {
		mp.NotifyTxsAvailable()
	})
}

func TestProxyMempool_ReapReturnsNil(t *testing.T) {
	conn := newMockAppConn(t)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	t.Run("ReapMaxBytesMaxGas returns nil", func(t *testing.T) {
		result := mp.ReapMaxBytesMaxGas(1000, 1000)
		require.Nil(t, result)
	})

	t.Run("ReapMaxTxs returns nil", func(t *testing.T) {
		result := mp.ReapMaxTxs(100)
		require.Nil(t, result)
	})
}

func TestProxyMempool_Height(t *testing.T) {
	conn := newMockAppConn(t)

	t.Run("initial height", func(t *testing.T) {
		mp := NewProxyMempool(newTestConfig(), conn, 42)
		mp.SetLogger(log.NewNopLogger())
		require.Equal(t, int64(42), mp.Height())
	})

	t.Run("updated after Update", func(t *testing.T) {
		mp := NewProxyMempool(newTestConfig(), conn, 0)
		mp.SetLogger(log.NewNopLogger())
		mp.Update(99, nil, nil, nil, nil)
		require.Equal(t, int64(99), mp.Height())
	})
}

func TestProxyMempool_IsIncludedTx(t *testing.T) {
	conn := newMockAppConn(t)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	tx := types.Tx("included")

	require.False(t, mp.IsIncludedTx(tx))

	mp.includedTxCache.Push(tx)
	require.True(t, mp.IsIncludedTx(tx))
}

func TestProxyMempool_AppEventCh(t *testing.T) {
	conn := newMockAppConn(t)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	ch := mp.AppEventCh()
	require.NotNil(t, ch)

	require.Equal(t, 8192, cap(ch))
}

func TestProxyMempool_LockUnlock(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncOK(conn)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	mp.Lock()
	mp.Unlock()

	err := mp.CheckTx(types.Tx("after-unlock"), nil, TxInfo{})
	require.NoError(t, err)
}

func TestProxyMempool_ConcurrentCheckTx(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncOK(conn)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	var wg sync.WaitGroup
	numGoroutines := 20
	txsPerGoroutine := 10

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < txsPerGoroutine; i++ {
				tx := types.Tx(fmt.Sprintf("concurrent-%d-%d", id, i))
				_ = mp.CheckTx(tx, nil, TxInfo{SenderP2PID: "peer"})
			}
		}(g)
	}
	wg.Wait()

	drainAppEventCh(mp)

	require.Equal(t, numGoroutines*txsPerGoroutine, mp.Size())
}

func TestProxyMempool_ConcurrentCheckTxAndUpdate(t *testing.T) {
	conn := newMockAppConn(t)
	setupCheckTxAsyncOK(conn)

	mp := NewProxyMempool(newTestConfig(), conn, 0)
	mp.SetLogger(log.NewNopLogger())

	var wg sync.WaitGroup

	// goroutine 1: submit txs
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			tx := types.Tx(fmt.Sprintf("race-tx-%d", i))
			_ = mp.CheckTx(tx, nil, TxInfo{})
		}
	}()

	// goroutine 2: concurrent Updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			mp.Lock()
			mp.Update(int64(i+1), nil, nil, nil, nil)
			mp.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	// should get here without deadlock
}

func TestProxyMempool_WithMetrics(t *testing.T) {
	conn := newMockAppConn(t)

	metrics := NopMetrics()
	mp := NewProxyMempool(newTestConfig(), conn, 0, WithProxyMempoolMetrics(metrics))
	mp.SetLogger(log.NewNopLogger())

	require.Equal(t, metrics, mp.metrics)
}

func TestProxyMempool_DefaultCacheSize(t *testing.T) {
	conn := newMockAppConn(t)

	cfg := newTestConfig()
	cfg.CacheSize = 0

	mp := NewProxyMempool(cfg, conn, 0)
	mp.SetLogger(log.NewNopLogger())

	require.NotNil(t, mp.includedTxCache)
}

func TestProxyMempool_EventDroppedWhenFull(t *testing.T) {
	conn := newMockAppConn(t)

	cfg := newTestConfig()
	mp := NewProxyMempool(cfg, conn, 0)
	mp.SetLogger(log.NewNopLogger())

	for i := 0; i < 8192; i++ {
		mp.appEventCh <- AppMempoolEvent{Type: EventTxQueued}
	}

	setupCheckTxAsyncOK(conn)
	err := mp.CheckTx(types.Tx("overflow-tx"), nil, TxInfo{})
	require.NoError(t, err)

	require.Equal(t, 1, mp.Size())
}

// drainAppEventCh drains all pending events from the ProxyMempool's event channel.
func drainAppEventCh(mp *ProxyMempool) {
	for {
		select {
		case <-mp.AppEventCh():
		default:
			return
		}
	}
}
