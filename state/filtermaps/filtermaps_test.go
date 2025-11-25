package filtermaps

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/store"
)

// Test that loadHeadSnapshot honors the indexed block range when the chain
// starts mid-height (e.g. state sync) and does not try to read pointers for
// earlier blocks.
func TestLoadHeadSnapshotRespectsIndexedRangeBase(t *testing.T) {
	params := Params{
		logMapHeight:       4,
		logMapWidth:        4,
		logMapsPerEpoch:    1,
		logValuesPerMap:    4,
		baseRowGroupLength: 4,
		baseRowLengthRatio: 2,
		logLayerDiff:       2,
	}
	params.deriveFields()

	fmCache := lru.NewCache[uint32, filterMap](cachedFilterMaps)
	lastBlockCache := lru.NewCache[uint32, uint64](cachedLastBlocks)
	lvPointerCache := lru.NewCache[uint64, uint64](cachedLvPointers)
	eventsPointersCache := lru.NewCache[uint64, []uint64](cachedEventsPointers)
	renderSnapshots := lru.NewCache[uint64, *renderedMap](cachedRenderSnapshots)

	baseBlock := uint64(100)
	prevLastBlock := uint64(103)
	headLastBlock := uint64(107)

	f := &FilterMaps{
		db:                  dbm.NewMemDB(),
		blockStore:          &store.BlockStore{},
		Params:              params,
		filterMapCache:      fmCache,
		lastBlockCache:      lastBlockCache,
		lvPointerCache:      lvPointerCache,
		eventsPointersCache: eventsPointersCache,
		renderSnapshots:     renderSnapshots,
		indexedRange: filterMapsRange{
			initialized: true,
			headIndexed: true,
			maps:        common.NewRange(uint32(5), 2), // head map index = 6
			blocks:      common.NewRange(baseBlock, headLastBlock-baseBlock+1),
		},
	}

	headMapIndex := f.indexedRange.maps.Last()
	headMap := make(filterMap, params.mapHeight)
	f.filterMapCache.Add(headMapIndex, headMap)

	f.lastBlockCache.Add(headMapIndex-1, prevLastBlock)
	f.lastBlockCache.Add(headMapIndex, headLastBlock)

	for b := prevLastBlock + 1; b <= headLastBlock; b++ {
		f.lvPointerCache.Add(b, b*10)             // arbitrary but present
		f.eventsPointersCache.Add(b, []uint64{0}) // minimal pointer list
	}

	if err := f.loadHeadSnapshot(); err != nil {
		t.Fatalf("loadHeadSnapshot returned error: %v", err)
	}

	snapshot, ok := f.renderSnapshots.Get(f.indexedRange.blocks.Last())
	if !ok || snapshot == nil {
		t.Fatalf("expected head snapshot cached at block %d", f.indexedRange.blocks.Last())
	}

	expectedFirstBlock := prevLastBlock + 1
	if expectedFirstBlock < f.indexedRange.blocks.First() {
		expectedFirstBlock = f.indexedRange.blocks.First()
	}
	if got := snapshot.firstBlock(); got != expectedFirstBlock {
		t.Fatalf("snapshot first block mismatch: got %d want %d", got, expectedFirstBlock)
	}
	if len(snapshot.blockLvPtrs) != int(headLastBlock-expectedFirstBlock+1) {
		t.Fatalf("snapshot block pointers length mismatch: got %d", len(snapshot.blockLvPtrs))
	}
}
