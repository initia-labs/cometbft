package mempool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	cmtsync "github.com/cometbft/cometbft/libs/sync"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	// admissionCooldownBaseTTL is the first local reject window after the app
	// mempool removes a tx. It should be long enough to absorb immediate peer
	// regossip, but short enough that a tx which later becomes valid is not
	// hidden from this node for long.
	admissionCooldownBaseTTL = 5 * time.Second

	// admissionCooldownMaxTTL caps the adaptive reject window for a single tx
	// hash. Repeated removals can extend the cooldown up to this bound.
	admissionCooldownMaxTTL = 5 * time.Minute

	// admissionCooldownMaxRemovals bounds the left shift used for exponential
	// TTL growth even though admissionCooldownMaxTTL is the effective duration
	// cap. Keeping both caps avoids overflow if this constant changes later.
	admissionCooldownMaxRemovals = 16
)

type admissionCooldownEntry struct {
	removals int
	expiry   time.Time
}

// ProxyMempool implements the Mempool interface as a thin passthrough.
// By using this ProxyMempool the CometBFT becomes a gossip layer while the application owns the real mempool.
type ProxyMempool struct {
	height atomic.Int64

	// exclusive Lock for Update, RLock for CheckTx
	updateMtx cmtsync.RWMutex

	config       *config.MempoolConfig
	proxyAppConn proxy.AppConnMempool

	// buffered channel the application pushes events into and the reactor consumes from
	appEventCh chan AppMempoolEvent

	// dedup cache, txKey -> known entry
	knownTxs     sync.Map
	knownTxCount atomic.Int64
	knownTxBytes atomic.Int64

	// in-flight CheckTx guard: txKey -> struct{} while the abci call is pending
	inCheckTxs sync.Map

	txsAvailable         chan struct{}
	notifiedTxsAvailable atomic.Bool
	hasValidTxs          atomic.Bool

	// LRU cache of committed tx hashes, populated during Update so that gossip of committed txs is rejected locally
	includedTxCache *LRUTxCache

	// admissionCooldown rejects exact tx hashes that were recently removed from
	// the application mempool. This prevents an evict-readmit loop where peers
	// immediately regossip the same bytes after the app has decided to remove
	// them. It is deliberately separate from includedTxCache: includedTxCache is
	// for committed txs, while admissionCooldown is a short local backoff for
	// txs the app mempool just dropped.
	admissionCooldownMtx sync.Mutex
	admissionCooldown    *lru.Cache[types.TxKey, *admissionCooldownEntry]

	logger  log.Logger
	metrics *Metrics
}

var _ Mempool = &ProxyMempool{}

// NewProxyMempool creates a new ProxyMempool.
func NewProxyMempool(
	cfg *config.MempoolConfig,
	proxyAppConn proxy.AppConnMempool,
	height int64,
	options ...func(*ProxyMempool),
) *ProxyMempool {
	cacheSize := cfg.CacheSize
	if cacheSize <= 0 {
		cacheSize = 10000
	}

	mp := &ProxyMempool{
		config:          cfg,
		proxyAppConn:    proxyAppConn,
		appEventCh:      make(chan AppMempoolEvent, 8192),
		includedTxCache: NewLRUTxCache(cacheSize),
		logger:          log.NewNopLogger(),
		metrics:         NopMetrics(),
	}
	mp.admissionCooldown = newAdmissionCooldown(cacheSize)
	mp.height.Store(height)

	// no-op callback so the local client doesn't panic
	proxyAppConn.SetResponseCallback(func(req *abci.Request, res *abci.Response) {})

	for _, opt := range options {
		opt(mp)
	}

	return mp
}

func newAdmissionCooldown(cacheSize int) *lru.Cache[types.TxKey, *admissionCooldownEntry] {
	if cacheSize <= 0 {
		cacheSize = 10000
	}
	cache, err := lru.New[types.TxKey, *admissionCooldownEntry](cacheSize)
	if err != nil {
		panic(err)
	}
	return cache
}

// WithProxyMempoolMetrics sets the metrics on the ProxyMempool.
func WithProxyMempoolMetrics(metrics *Metrics) func(*ProxyMempool) {
	return func(mp *ProxyMempool) { mp.metrics = metrics }
}

// SetLogger sets the logger.
func (mp *ProxyMempool) SetLogger(l log.Logger) {
	mp.logger = l
}

// AppEventCh returns the channel the reactor should consume.
func (mp *ProxyMempool) AppEventCh() chan AppMempoolEvent {
	return mp.appEventCh
}

// CheckTx forwards the transaction to the application via ABCI CheckTx.
func (mp *ProxyMempool) CheckTx(
	tx types.Tx,
	cb func(*abci.ResponseCheckTx),
	txInfo TxInfo,
) error {
	mp.updateMtx.RLock()
	defer mp.updateMtx.RUnlock()

	txSize := len(tx)
	if txSize > mp.config.MaxTxBytes {
		return ErrTxTooLarge{
			Max:    mp.config.MaxTxBytes,
			Actual: txSize,
		}
	}

	txKey := tx.Key()

	if _, ok := mp.knownTxs.Load(txKey); ok {
		return ErrTxInCache
	}

	if mp.includedTxCache.Has(tx) {
		return ErrTxInCache
	}

	// If the app mempool recently removed these exact bytes, do not call
	// CheckTx again yet. Returning ErrTxInCache keeps peer-originated retries on
	// the existing quiet duplicate path and prevents a local readmit/regossip
	// cycle while the cooldown is active.
	if mp.isAdmissionCoolingDown(txKey) {
		return ErrTxInCache
	}

	// prevent duplicate in-flight CheckTx calls for the same tx
	if _, loaded := mp.inCheckTxs.LoadOrStore(txKey, struct{}{}); loaded {
		return ErrTxInCache
	}

	reqRes, err := mp.proxyAppConn.CheckTxAsync(context.TODO(), &abci.RequestCheckTx{Tx: tx})
	if err != nil {
		mp.inCheckTxs.Delete(txKey)
		return ErrAppConnMempool{Err: err}
	}

	reqRes.SetCallback(func(res *abci.Response) {
		mp.inCheckTxs.Delete(txKey)

		checkTxRes := res.GetCheckTx()
		if checkTxRes == nil {
			return
		}

		if checkTxRes.Code == abci.CodeTypeOK {
			if _, loaded := mp.knownTxs.LoadOrStore(txKey, tx); !loaded {
				mp.knownTxCount.Add(1)
				mp.knownTxBytes.Add(int64(txSize))

				select {
				case mp.appEventCh <- AppMempoolEvent{
					Type:     EventTxQueued,
					TxKey:    txKey,
					Tx:       tx,
					SenderID: txInfo.SenderP2PID,
				}:
				default:
					mp.logger.Error("appEventCh full, dropping EventTxQueued")
				}
			}
		}

		if cb != nil {
			cb(checkTxRes)
		}
	})

	return nil
}

// AddAdmissionCooldown records that the application mempool removed this tx
// hash and asks CheckTx to reject the same bytes locally for a short period.
//
// The cooldown is adaptive per hash: one removal gets the base TTL, and
// repeated removals before the previous cooldown expires extend the TTL
// exponentially up to a cap. The LRU size bound keeps both active cooldowns and
// repeated-removal counters bounded by the node's normal tx cache size.
func (mp *ProxyMempool) AddAdmissionCooldown(txKey types.TxKey) {
	now := time.Now()

	mp.admissionCooldownMtx.Lock()
	defer mp.admissionCooldownMtx.Unlock()

	entry, ok := mp.admissionCooldown.Get(txKey)
	if !ok {
		entry = &admissionCooldownEntry{}
	} else if !entry.expiry.After(now) {
		entry.removals = 0
	}

	entry.removals++

	attempts := min(entry.removals-1, admissionCooldownMaxRemovals)
	ttl := min(admissionCooldownBaseTTL<<attempts, admissionCooldownMaxTTL)
	entry.expiry = now.Add(ttl)
	mp.admissionCooldown.Add(txKey, entry)
}

func (mp *ProxyMempool) isAdmissionCoolingDown(txKey types.TxKey) bool {
	mp.admissionCooldownMtx.Lock()
	defer mp.admissionCooldownMtx.Unlock()

	entry, ok := mp.admissionCooldown.Get(txKey)
	if !ok {
		return false
	}
	if entry.expiry.After(time.Now()) {
		return true
	}

	mp.admissionCooldown.Remove(txKey)
	return false
}

// RemoveTxByKey removes a transaction from the knownTxs cache.
func (mp *ProxyMempool) RemoveTxByKey(txKey types.TxKey) error {
	if entry, ok := mp.knownTxs.LoadAndDelete(txKey); ok {
		mp.knownTxCount.Add(-1)
		mp.knownTxBytes.Add(-int64(len(entry.(types.Tx))))
		return nil
	}

	return ErrTxNotFound
}

// ReapMaxBytesMaxGas returns nil. The app provides txs via PrepareProposal.
func (mp *ProxyMempool) ReapMaxBytesMaxGas(_, _ int64) types.Txs {
	return nil
}

// ReapMaxTxs returns nil.
func (mp *ProxyMempool) ReapMaxTxs(_ int) types.Txs {
	return nil
}

// Lock acquires the exclusive update lock.
func (mp *ProxyMempool) Lock() {
	mp.updateMtx.Lock()
}

// Unlock releases the exclusive update lock.
func (mp *ProxyMempool) Unlock() {
	mp.updateMtx.Unlock()
}

// Update stores the new height and removes committed txs from knownTxs.
func (mp *ProxyMempool) Update(
	blockHeight int64,
	blockTxs types.Txs,
	txResults []*abci.ExecTxResult,
	_ PreCheckFunc,
	_ PostCheckFunc,
) error {
	mp.height.Store(blockHeight)
	mp.notifiedTxsAvailable.Store(false)

	for idx, tx := range blockTxs {
		if txResults[idx].Code == abci.CodeTypeOK || mp.config.KeepInvalidTxsInCache {
			mp.includedTxCache.Push(tx) // cache the valid committed tx
		}
		mp.RemoveTxByKey(tx.Key())
	}

	// renotify if there are still valid txs from before this block.
	if mp.hasValidTxs.Load() {
		mp.notifyTxsAvailable()
	}

	mp.metrics.Size.Set(float64(mp.Size()))
	mp.metrics.SizeBytes.Set(float64(mp.SizeBytes()))

	return nil
}

// FlushAppConn flushes the mempool ABCI connection.
func (mp *ProxyMempool) FlushAppConn() error {
	if err := mp.proxyAppConn.Flush(context.TODO()); err != nil {
		return ErrFlushAppConn{Err: err}
	}

	return nil
}

// Flush clears all mempool state.
func (mp *ProxyMempool) Flush() {
	mp.knownTxs.Range(func(key, _ interface{}) bool {
		mp.knownTxs.Delete(key)
		return true
	})
	mp.knownTxCount.Store(0)
	mp.knownTxBytes.Store(0)
	mp.includedTxCache.Reset()
	mp.hasValidTxs.Store(false)
	mp.inCheckTxs.Range(func(key, _ interface{}) bool {
		mp.inCheckTxs.Delete(key)
		return true
	})
	mp.admissionCooldownMtx.Lock()
	mp.admissionCooldown.Purge()
	mp.admissionCooldownMtx.Unlock()
}

// TxsAvailable returns a channel that fires once per height when transactions are available in the mempool.
func (mp *ProxyMempool) TxsAvailable() <-chan struct{} {
	return mp.txsAvailable
}

// EnableTxsAvailable initializes the TxsAvailable channel.
func (mp *ProxyMempool) EnableTxsAvailable() {
	mp.txsAvailable = make(chan struct{}, 1)
}

// Size returns the number of known transactions.
func (mp *ProxyMempool) Size() int {
	return int(mp.knownTxCount.Load())
}

// SizeBytes returns the total size of known transactions in bytes.
func (mp *ProxyMempool) SizeBytes() int64 {
	return mp.knownTxBytes.Load()
}

// HasValidTxs returns whether any valid tx has been inserted since the last Update.
func (mp *ProxyMempool) HasValidTxs() bool {
	return mp.hasValidTxs.Load()
}

// SetHasValidTxs sets the hasValidTxs flag. Called by the reactor on EventTxInserted.
func (mp *ProxyMempool) SetHasValidTxs(v bool) {
	mp.hasValidTxs.Store(v)
}

// notifyTxsAvailable fires the TxsAvailable channel if not already notified.
func (mp *ProxyMempool) notifyTxsAvailable() {
	if mp.txsAvailable != nil && mp.notifiedTxsAvailable.CompareAndSwap(false, true) {
		select {
		case mp.txsAvailable <- struct{}{}:
		default:
		}
	}
}

// NotifyTxsAvailable is called by the reactor when EventTxInserted arrives.
func (mp *ProxyMempool) NotifyTxsAvailable() {
	mp.notifyTxsAvailable()
}

// IsIncludedTx returns true if the tx was committed in a recent block.
func (mp *ProxyMempool) IsIncludedTx(tx types.Tx) bool {
	return mp.includedTxCache.Has(tx)
}

// Height returns the last block height passed to Update.
func (mp *ProxyMempool) Height() int64 {
	return mp.height.Load()
}
