// Copyright 2024 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package filtermaps

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/libs/log"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
)

const (
	databaseVersion       = 2    // reindexed if database version does not match
	cachedLastBlocks      = 1000 // last block of map pointers
	cachedLvPointers      = 1000 // first log value pointer of block pointers
	cachedFilterMaps      = 3    // complete filter maps (cached by map renderer)
	cachedRenderSnapshots = 8    // saved map renderer data at block boundaries
)

// FilterMaps is the in-memory representation of the log index structure that is
// responsible for building and updating the index according to the canonical
// chain.
//
// Note that FilterMaps implements the same data structure as proposed in EIP-7745
// without the tree hashing and consensus changes:
// https://eips.ethereum.org/EIPS/eip-7745
type FilterMaps struct {
	// If disabled is set, log indexing is fully disabled.
	// This is configured by the --history.logs.disable Geth flag.
	// We chose to implement disabling this way because it requires less special
	// case logic in eth/filters.
	disabled   bool
	disabledCh chan struct{} // closed by indexer if disabled

	closeCh        chan struct{}
	closeWg        sync.WaitGroup
	history        uint64
	hashScheme     bool // use hashdb-safe delete range method
	exportFileName string
	Params

	db         dbm.DB
	blockStore *store.BlockStore
	stateStore sm.Store
	logger     log.Logger

	// fields written by the indexer and read by matcher backend. Indexer can
	// read them without a lock and write them under indexLock write lock.
	// Matcher backend can read them under indexLock read lock.
	indexLock     sync.RWMutex
	indexedRange  filterMapsRange
	indexedHeight uint64 // always consistent with the log index
	hasTempRange  bool

	// cleanedEpochsBefore indicates that all unindexed data before this point
	// has been cleaned.
	//
	// This field is only accessed and modified within tryUnindexTail, so no
	// explicit locking is required.
	cleanedEpochsBefore uint32

	// also accessed by indexer and matcher backend but no locking needed.
	filterMapCache *lru.Cache[uint32, filterMap]
	lastBlockCache *lru.Cache[uint32, uint64]
	lvPointerCache *lru.Cache[uint64, uint64]

	// the matchers set and the fields of FilterMapsMatcherBackend instances are
	// read and written both by exported functions and the indexer.
	// Note that if both indexLock and matchersLock needs to be locked then
	// indexLock should be locked first.
	matchersLock sync.Mutex
	matchers     map[*FilterMapsMatcherBackend]struct{}

	// fields only accessed by the indexer (no mutex required).
	renderSnapshots                                              *lru.Cache[uint64, *renderedMap]
	startedHeadIndex, startedTailIndex, startedTailUnindex       bool
	startedHeadIndexAt, startedTailIndexAt, startedTailUnindexAt time.Time
	loggedHeadIndex, loggedTailIndex                             bool
	lastLogHeadIndex, lastLogTailIndex                           time.Time
	ptrHeadIndex, ptrTailIndex, ptrTailUnindexBlock              uint64
	ptrTailUnindexMap                                            uint32

	targetHeight        uint64
	matcherSyncRequests []*FilterMapsMatcherBackend
	historyCutoff       uint64
	lastFinal           uint64
	lastFinalEpoch      uint32
	stop                bool
	targetCh            chan targetUpdate
	blockProcessingCh   chan bool
	blockProcessing     bool
	matcherSyncCh       chan *FilterMapsMatcherBackend
	waitIdleCh          chan chan bool
	tailRenderer        *mapRenderer

	// test hooks
	testDisableSnapshots, testSnapshotUsed bool
	testProcessEventsHook                  func()
}

// filterMap is a full or partial in-memory representation of a filter map where
// rows are allowed to have a nil value meaning the row is not stored in the
// structure. Note that therefore a known empty row should be represented with
// a zero-length slice.
// It can be used as a memory cache or an overlay while preparing a batch of
// changes to the structure. In either case a nil value should be interpreted
// as transparent (uncached/unchanged).
type filterMap []FilterRow

// fastCopy returns a copy of the given filter map. Note that the row slices are
// copied but their contents are not. This permits appending to the rows further
// (which happens during map rendering) without affecting the validity of
// copies made for snapshots during rendering.
// Appending to the rows of both the original map and the fast copy, or two fast
// copies of the same map would result in data corruption, therefore a fast copy
// should always be used in a read only way.
func (fm filterMap) fastCopy() filterMap {
	return slices.Clone(fm)
}

// fullCopy returns a copy of the given filter map, also making a copy of each
// individual filter row, ensuring that a modification to either one will never
// affect the other.
func (fm filterMap) fullCopy() filterMap {
	c := make(filterMap, len(fm))
	for i, row := range fm {
		c[i] = slices.Clone(row)
	}
	return c
}

// FilterRow encodes a single row of a filter map as a list of column indices.
// Note that the values are always stored in the same order as they were added
// and if the same column index is added twice, it is also stored twice.
// Order of column indices and potential duplications do not matter when searching
// for a value but leaving the original order makes reverting to a previous state
// simpler.
type FilterRow []uint32

// Equal returns true if the given filter rows are equivalent.
func (a FilterRow) Equal(b FilterRow) bool {
	return slices.Equal(a, b)
}

// filterMapsRange describes the rendered range of filter maps and the range
// of fully rendered blocks.
type filterMapsRange struct {
	initialized   bool
	headIndexed   bool
	headDelimiter uint64 // zero if headIndexed is false
	// if initialized then all maps are rendered in the maps range
	maps common.Range[uint32]
	// if tailPartialEpoch > 0 then maps between firstRenderedMap-mapsPerEpoch and
	// firstRenderedMap-mapsPerEpoch+tailPartialEpoch-1 are rendered
	tailPartialEpoch uint32
	// if initialized then all log values in the blocks range are fully
	// rendered
	// blockLvPointers are available in the blocks range
	blocks common.Range[uint64]
}

// hasIndexedBlocks returns true if the range has at least one fully indexed block.
func (fmr *filterMapsRange) hasIndexedBlocks() bool {
	return fmr.initialized && !fmr.blocks.IsEmpty() && !fmr.maps.IsEmpty()
}

// lastBlockOfMap is used for caching the (number, id) pairs belonging to the
// last block of each map.
type lastBlockOfMap struct {
	number uint64
	id     common.Hash
}

// Config contains the configuration options for NewFilterMaps.
type Config struct {
	History  uint64 // number of historical blocks to index
	Disabled bool   // disables indexing completely

	// This option enables the checkpoint JSON file generator.
	// If set, the given file will be updated with checkpoint information.
	ExportFileName string

	// expect trie nodes of hash based state scheme in the filtermaps key range;
	// use safe iterator based implementation of DeleteRange that skips them
	HashScheme bool
}

// NewFilterMaps creates a new FilterMaps and starts the indexer.
func NewFilterMaps(db dbm.DB, blockStore *store.BlockStore, stateStore sm.Store, historyCutoff uint64, params Params, config Config) *FilterMaps {
	rs, initialized, err := ReadFilterMapsRange(db)
	if err != nil || (initialized && rs.Version != databaseVersion) {
		rs, initialized = FilterMapsRange{}, false
	}
	params.deriveFields()
	f := &FilterMaps{
		db:         db,
		blockStore: blockStore,
		stateStore: stateStore,

		closeCh:           make(chan struct{}),
		waitIdleCh:        make(chan chan bool),
		targetCh:          make(chan targetUpdate, 1),
		blockProcessingCh: make(chan bool, 1),
		history:           config.History,
		disabled:          config.Disabled,
		hashScheme:        config.HashScheme,
		disabledCh:        make(chan struct{}),
		exportFileName:    config.ExportFileName,
		Params:            params,
		targetHeight:      rs.BlocksAfterLast,
		indexedHeight:     rs.BlocksAfterLast,
		indexedRange: filterMapsRange{
			initialized:      initialized,
			headIndexed:      rs.HeadIndexed,
			headDelimiter:    rs.HeadDelimiter,
			blocks:           common.NewRange(rs.BlocksFirst, rs.BlocksAfterLast-rs.BlocksFirst),
			maps:             common.NewRange(rs.MapsFirst, rs.MapsAfterLast-rs.MapsFirst),
			tailPartialEpoch: rs.TailPartialEpoch,
		},
		// deleting last unindexed epoch might have been interrupted by shutdown
		cleanedEpochsBefore: max(rs.MapsFirst>>params.logMapsPerEpoch, 1) - 1,

		historyCutoff:   historyCutoff,
		matcherSyncCh:   make(chan *FilterMapsMatcherBackend),
		matchers:        make(map[*FilterMapsMatcherBackend]struct{}),
		filterMapCache:  lru.NewCache[uint32, filterMap](cachedFilterMaps),
		lastBlockCache:  lru.NewCache[uint32, uint64](cachedLastBlocks),
		lvPointerCache:  lru.NewCache[uint64, uint64](cachedLvPointers),
		renderSnapshots: lru.NewCache[uint64, *renderedMap](cachedRenderSnapshots),
	}
	f.checkRevertRange() // revert maps that are inconsistent with the current chain view
	return f
}

func (f *FilterMaps) SetLogger(l log.Logger) {
	f.logger = l

	if f.indexedRange.hasIndexedBlocks() {
		f.logger.Info("Initialized log indexer",
			"first block", f.indexedRange.blocks.First(), "last block", f.indexedRange.blocks.Last(),
			"first map", f.indexedRange.maps.First(), "last map", f.indexedRange.maps.Last(),
			"head indexed", f.indexedRange.headIndexed)
	}
}

// Start starts the indexer.
func (f *FilterMaps) Start() {
	if !f.testDisableSnapshots && f.indexedRange.hasIndexedBlocks() && f.indexedRange.headIndexed {
		// previous target head rendered; load last map as snapshot
		if err := f.loadHeadSnapshot(); err != nil {
			f.logger.Error("Could not load head filter map snapshot", "error", err)
		}
	}
	f.closeWg.Add(1)
	go f.indexerLoop()
}

// Stop ensures that the indexer is fully stopped before returning.
func (f *FilterMaps) Stop() {
	close(f.closeCh)
	f.closeWg.Wait()
}

// init initializes an empty log index according to the current targetView.
func (f *FilterMaps) init() error {
	batch := f.db.NewBatch()
	fmr := filterMapsRange{
		initialized: true,
	}
	err := f.setRange(batch, f.targetHeight, fmr, false)
	if err != nil {
		return err
	}
	return batch.Write()
}

// checkRevertRange checks whether the existing index is consistent with the
// current indexed view and reverts inconsistent maps if necessary.
func (f *FilterMaps) checkRevertRange() {
	if f.indexedRange.maps.Count() == 0 {
		return
	}
	lastMap := f.indexedRange.maps.Last()
	lastBlockNumber, err := f.getLastBlockOfMap(lastMap)
	if err != nil {
		f.logger.Error("Error initializing log index database; resetting log index", "error", err)
		f.reset()
		return
	}
	for lastBlockNumber > f.indexedHeight {
		// revert last map
		if f.indexedRange.maps.Count() == 1 {
			f.reset() // reset database if no rendered maps remained
			return
		}
		lastMap--
		newRange := f.indexedRange
		newRange.maps.SetLast(lastMap)
		lastBlockNumber, err = f.getLastBlockOfMap(lastMap)
		if err != nil {
			f.logger.Error("Error initializing log index database; resetting log index", "error", err)
			f.reset()
			return
		}
		newRange.blocks.SetAfterLast(lastBlockNumber) // lastBlockNumber is probably partially indexed
		newRange.headIndexed = false
		newRange.headDelimiter = 0
		// only shorten range and leave map data; next head render will overwrite it
		batch := f.db.NewBatch()
		err = f.setRange(batch, f.indexedHeight, newRange, false)
		if err != nil {
			f.logger.Error("Error setting log index database range", "error", err)
			return
		}
		err = batch.Write()
		if err != nil {
			f.logger.Error("Error writing log index database batch", "error", err)
			return
		}
	}
}

// reset un-initializes the FilterMaps structure and removes all related data from
// the database.
// Note that in case of leveldb database the fallback implementation of DeleteRange
// might take a long time to finish and deleting the entire database may be
// interrupted by a shutdown. Deleting the filterMapsRange entry first does
// guarantee though that the next init() will not return successfully until the
// entire database has been cleaned.
func (f *FilterMaps) reset() error {
	f.indexLock.Lock()
	f.indexedRange = filterMapsRange{}
	f.indexedHeight = 0
	f.filterMapCache.Purge()
	f.renderSnapshots.Purge()
	f.lastBlockCache.Purge()
	f.lvPointerCache.Purge()
	f.indexLock.Unlock()
	// deleting the range first ensures that resetDb will be called again at next
	// startup and any leftover data will be removed even if it cannot finish now.
	batch := f.db.NewBatch()
	err := DeleteFilterMapsRange(batch)
	if err != nil {
		return err
	}
	err = batch.Write()
	if err != nil {
		return err
	}
	return f.safeDeleteWithLogs(DeleteFilterMapsDb, "Resetting log index database", f.isShuttingDown)
}

// isShuttingDown returns true if FilterMaps is shutting down.
func (f *FilterMaps) isShuttingDown() bool {
	select {
	case <-f.closeCh:
		return true
	default:
		return false
	}
}

// safeDeleteWithLogs is a wrapper for a function that performs a safe range
// delete operation using rawdb.SafeDeleteRange. It emits log messages if the
// process takes long enough to call the stop callback.
func (f *FilterMaps) safeDeleteWithLogs(deleteFn func(db dbm.DB, hashScheme bool, stopCb func(bool) bool) error, action string, stopCb func() bool) error {
	var (
		start          = time.Now()
		logPrinted     bool
		lastLogPrinted = start
	)
	switch err := deleteFn(f.db, f.hashScheme, func(deleted bool) bool {
		if deleted && !logPrinted || time.Since(lastLogPrinted) > time.Second*10 {
			f.logger.Info(action+" in progress...", "elapsed", common.PrettyDuration(time.Since(start)))
			logPrinted, lastLogPrinted = true, time.Now()
		}
		return stopCb()
	}); {
	case err == nil:
		if logPrinted {
			f.logger.Info(action+" finished", "elapsed", common.PrettyDuration(time.Since(start)))
		}
		return nil
	default:
		f.logger.Error(action+" failed", "error", err)
		return err
	}
}

// setRange updates the indexed chain view and covered range and also adds the
// changes to the given batch.
//
// Note that this function assumes that the index write lock is being held.
func (f *FilterMaps) setRange(batch dbm.Batch, newHeight uint64, newRange filterMapsRange, isTempRange bool) error {
	f.indexedHeight = newHeight
	f.indexedRange = newRange
	f.hasTempRange = isTempRange
	f.updateMatchersValidRange()
	if newRange.initialized {
		rs := FilterMapsRange{
			Version:          databaseVersion,
			HeadIndexed:      newRange.headIndexed,
			HeadDelimiter:    newRange.headDelimiter,
			BlocksFirst:      newRange.blocks.First(),
			BlocksAfterLast:  newRange.blocks.AfterLast(),
			MapsFirst:        newRange.maps.First(),
			MapsAfterLast:    newRange.maps.AfterLast(),
			TailPartialEpoch: newRange.tailPartialEpoch,
		}
		return WriteFilterMapsRange(batch, rs)
	} else {
		return DeleteFilterMapsRange(batch)
	}
}

// getLogByLvIndex returns the log at the given log value index. If the index does
// not point to the first log value entry of a log then no log and no error are
// returned as this can happen when the log value index was a false positive.
// Note that this function assumes that the log index structure is consistent
// with the canonical chain at the point where the given log value index points.
// If this is not the case then an invalid result or an error may be returned.
//
// Note that this function assumes that the indexer read lock is being held when
// called from outside the indexerLoop goroutine.
func (f *FilterMaps) getLogByLvIndex(lvIndex uint64) (*TxEvent, error) {
	mapIndex := uint32(lvIndex >> f.logValuesPerMap)
	if !f.indexedRange.maps.Includes(mapIndex) {
		return nil, nil
	}
	// find possible block range based on map to block pointers
	lastBlockNumber, err := f.getLastBlockOfMap(mapIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve last block of map %d containing searched log value index %d: %v", mapIndex, lvIndex, err)
	}
	var firstBlockNumber uint64
	if mapIndex > 0 {
		firstBlockNumber, err = f.getLastBlockOfMap(mapIndex - 1)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve last block of map %d before searched log value index %d: %v", mapIndex, lvIndex, err)
		}
	}
	if firstBlockNumber < f.indexedRange.blocks.First() {
		firstBlockNumber = f.indexedRange.blocks.First()
	}
	// find block with binary search based on block to log value index pointers
	for firstBlockNumber < lastBlockNumber {
		midBlockNumber := (firstBlockNumber + lastBlockNumber + 1) / 2
		midLvPointer, err := f.getBlockLvPointer(midBlockNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve log value pointer of block %d while binary searching log value index %d: %v", midBlockNumber, lvIndex, err)
		}
		if lvIndex < midLvPointer {
			lastBlockNumber = midBlockNumber - 1
		} else {
			firstBlockNumber = midBlockNumber
		}
	}

	blockResponse, err := f.stateStore.LoadFinalizeBlockResponse(int64(firstBlockNumber + 1))
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve block response for block %d: %v", firstBlockNumber+1, err)
	}

	lvPointer, err := f.getBlockLvPointer(firstBlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve log value pointer of block %d containing searched log value index %d: %v", firstBlockNumber, lvIndex, err)
	}

	// iterate through receipts to find the exact log starting at lvIndex
	for txIndex, txResult := range blockResponse.TxResults {
		for _, event := range txResult.Events {
			l := uint64(len(event.Attributes))
			r := f.valuesPerMap - lvPointer%f.valuesPerMap
			if l > r {
				lvPointer += r // skip to map boundary
			}

			if lvPointer > lvIndex {
				// lvIndex does not point to the first log value (address value)
				// generated by a log as true matches should always do, so it
				// is considered a false positive (no log and no error returned).
				return nil, nil
			}
			if lvPointer <= lvIndex && lvIndex < lvPointer+l {
				return &TxEvent{
					BlockNumber: int64(firstBlockNumber + 1),
					TxIndex:     txIndex,
					Event:       txResult.Events,
				}, nil // potential match
			}
			lvPointer += l
		}
	}
	return nil, nil
}

// getFilterMap fetches an entire filter map from the database.
func (f *FilterMaps) getFilterMap(mapIndex uint32) (filterMap, error) {
	if fm, ok := f.filterMapCache.Get(mapIndex); ok {
		return fm, nil
	}
	fm := make(filterMap, f.mapHeight)
	for rowIndex := range fm {
		rows, err := f.getFilterMapRows([]uint32{mapIndex}, uint32(rowIndex), false)
		if err != nil {
			return nil, fmt.Errorf("failed to load filter map %d from database: %v", mapIndex, err)
		}
		fm[rowIndex] = rows[0]
	}
	f.filterMapCache.Add(mapIndex, fm)
	return fm, nil
}

// getFilterMapRows fetches a set of filter map rows at the corresponding map
// indices and a shared row index. If baseLayerOnly is true then only the first
// baseRowLength entries are returned.
func (f *FilterMaps) getFilterMapRows(mapIndices []uint32, rowIndex uint32, baseLayerOnly bool) ([]FilterRow, error) {
	rows := make([]FilterRow, len(mapIndices))
	var ptr int
	for len(mapIndices) > ptr {
		baseRowGroup := mapIndices[ptr] / f.baseRowGroupLength
		groupLength := 1
		for ptr+groupLength < len(mapIndices) && mapIndices[ptr+groupLength]/f.baseRowGroupLength == baseRowGroup {
			groupLength++
		}
		if err := f.getFilterMapRowsOfGroup(rows[ptr:ptr+groupLength], mapIndices[ptr:ptr+groupLength], rowIndex, baseLayerOnly); err != nil {
			return nil, err
		}
		ptr += groupLength
	}
	return rows, nil
}

// getFilterMapRowsOfGroup fetches a set of filter map rows at map indices
// belonging to the same base row group.
func (f *FilterMaps) getFilterMapRowsOfGroup(target []FilterRow, mapIndices []uint32, rowIndex uint32, baseLayerOnly bool) error {
	baseRowGroup := mapIndices[0] / f.baseRowGroupLength
	baseMapRowIndex := f.mapRowIndex(baseRowGroup*f.baseRowGroupLength, rowIndex)
	baseRows, err := ReadFilterMapBaseRows(f.db, baseMapRowIndex, f.baseRowGroupLength, f.logMapWidth)
	if err != nil {
		return fmt.Errorf("failed to retrieve base row group %d of row %d: %v", baseRowGroup, rowIndex, err)
	}
	for i, mapIndex := range mapIndices {
		if mapIndex/f.baseRowGroupLength != baseRowGroup {
			panic("mapIndices are not in the same base row group")
		}
		row := baseRows[mapIndex&(f.baseRowGroupLength-1)]
		if !baseLayerOnly {
			extRow, err := ReadFilterMapExtRow(f.db, f.mapRowIndex(mapIndex, rowIndex), f.logMapWidth)
			if err != nil {
				return fmt.Errorf("failed to retrieve filter map %d extended row %d: %v", mapIndex, rowIndex, err)
			}
			row = append(row, extRow...)
		}
		target[i] = row
	}
	return nil
}

// storeFilterMapRows stores a set of filter map rows at the corresponding map
// indices and a shared row index.
func (f *FilterMaps) storeFilterMapRows(batch dbm.Batch, mapIndices []uint32, rowIndex uint32, rows []FilterRow) error {
	for len(mapIndices) > 0 {
		baseRowGroup := mapIndices[0] / f.baseRowGroupLength
		groupLength := 1
		for groupLength < len(mapIndices) && mapIndices[groupLength]/f.baseRowGroupLength == baseRowGroup {
			groupLength++
		}
		if err := f.storeFilterMapRowsOfGroup(batch, mapIndices[:groupLength], rowIndex, rows[:groupLength]); err != nil {
			return err
		}
		mapIndices, rows = mapIndices[groupLength:], rows[groupLength:]
	}
	return nil
}

// storeFilterMapRowsOfGroup stores a set of filter map rows at map indices
// belonging to the same base row group.
func (f *FilterMaps) storeFilterMapRowsOfGroup(batch dbm.Batch, mapIndices []uint32, rowIndex uint32, rows []FilterRow) error {
	baseRowGroup := mapIndices[0] / f.baseRowGroupLength
	baseMapRowIndex := f.mapRowIndex(baseRowGroup*f.baseRowGroupLength, rowIndex)
	var baseRows [][]uint32
	if uint32(len(mapIndices)) != f.baseRowGroupLength { // skip base rows read if all rows are replaced
		var err error
		baseRows, err = ReadFilterMapBaseRows(f.db, baseMapRowIndex, f.baseRowGroupLength, f.logMapWidth)
		if err != nil {
			return fmt.Errorf("failed to retrieve base row group %d of row %d for modification: %v", baseRowGroup, rowIndex, err)
		}
	} else {
		baseRows = make([][]uint32, f.baseRowGroupLength)
	}
	for i, mapIndex := range mapIndices {
		if mapIndex/f.baseRowGroupLength != baseRowGroup {
			panic("mapIndices are not in the same base row group")
		}
		baseRow := []uint32(rows[i])
		var extRow FilterRow
		if uint32(len(rows[i])) > f.baseRowLength {
			extRow = baseRow[f.baseRowLength:]
			baseRow = baseRow[:f.baseRowLength]
		}
		baseRows[mapIndex&(f.baseRowGroupLength-1)] = baseRow
		err := WriteFilterMapExtRow(batch, f.mapRowIndex(mapIndex, rowIndex), extRow, f.logMapWidth)
		if err != nil {
			return fmt.Errorf("failed to store filter map %d extended row %d: %v", mapIndex, rowIndex, err)
		}
	}
	return WriteFilterMapBaseRows(batch, baseMapRowIndex, baseRows, f.logMapWidth)
}

// mapRowIndex calculates the unified storage index where the given row of the
// given map is stored. Note that this indexing scheme is the same as the one
// proposed in EIP-7745 for tree-hashing the filter map structure and for the
// same data proximity reasons it is also suitable for database representation.
// See also:
// https://eips.ethereum.org/EIPS/eip-7745#hash-tree-structure
func (f *FilterMaps) mapRowIndex(mapIndex, rowIndex uint32) uint64 {
	epochIndex, mapSubIndex := mapIndex>>f.logMapsPerEpoch, mapIndex&(f.mapsPerEpoch-1)
	return (uint64(epochIndex)<<f.logMapHeight+uint64(rowIndex))<<f.logMapsPerEpoch + uint64(mapSubIndex)
}

// getBlockLvPointer returns the starting log value index where the log values
// generated by the given block are located.
//
// Note that this function assumes that the indexer read lock is being held when
// called from outside the indexerLoop goroutine.
func (f *FilterMaps) getBlockLvPointer(blockNumber uint64) (uint64, error) {
	if lvPointer, ok := f.lvPointerCache.Get(blockNumber); ok {
		return lvPointer, nil
	}
	lvPointer, err := ReadBlockLvPointer(f.db, blockNumber)
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve log value pointer of block %d: %v", blockNumber, err)
	}
	f.lvPointerCache.Add(blockNumber, lvPointer)
	return lvPointer, nil
}

// storeBlockLvPointer stores the starting log value index where the log values
// generated by the given block are located.
func (f *FilterMaps) storeBlockLvPointer(batch dbm.Batch, blockNumber, lvPointer uint64) error {
	f.lvPointerCache.Add(blockNumber, lvPointer)
	return WriteBlockLvPointer(batch, blockNumber, lvPointer)
}

// deleteBlockLvPointer deletes the starting log value index where the log values
// generated by the given block are located.
func (f *FilterMaps) deleteBlockLvPointer(batch dbm.Batch, blockNumber uint64) error {
	f.lvPointerCache.Remove(blockNumber)
	return DeleteBlockLvPointer(batch, blockNumber)
}

// getLastBlockOfMap returns the number and id of the block that generated the
// last log value entry of the given map.
func (f *FilterMaps) getLastBlockOfMap(mapIndex uint32) (uint64, error) {
	if lastBlock, ok := f.lastBlockCache.Get(mapIndex); ok {
		return lastBlock, nil
	}
	number, err := ReadFilterMapLastBlock(f.db, mapIndex)
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve last block of map %d: %v", mapIndex, err)
	}
	f.lastBlockCache.Add(mapIndex, number)
	return number, nil
}

// storeLastBlockOfMap stores the number of the block that generated the last
// log value entry of the given map.
func (f *FilterMaps) storeLastBlockOfMap(batch dbm.Batch, mapIndex uint32, number uint64) error {
	f.lastBlockCache.Add(mapIndex, number)
	return WriteFilterMapLastBlock(batch, mapIndex, number)
}

// deleteLastBlockOfMap deletes the number of the block that generated the last
// log value entry of the given map.
func (f *FilterMaps) deleteLastBlockOfMap(batch dbm.Batch, mapIndex uint32) error {
	f.lastBlockCache.Remove(mapIndex)
	return DeleteFilterMapLastBlock(batch, mapIndex)
}

// deleteTailEpoch deletes index data from the specified epoch. The last block
// pointer for the last map of the epoch and the corresponding block log value
// pointer are retained as these are always assumed to be available for each
// epoch as boundary markers.
// The function returns true if all index data related to the epoch (except for
// the boundary markers) has been fully removed.
func (f *FilterMaps) deleteTailEpoch(epoch uint32) (bool, error) {
	f.indexLock.Lock()
	defer f.indexLock.Unlock()

	// determine epoch boundaries
	firstMap := epoch << f.logMapsPerEpoch
	lastBlock, err := f.getLastBlockOfMap(firstMap + f.mapsPerEpoch - 1)
	if err != nil {
		return false, fmt.Errorf("failed to retrieve last block of deleted epoch %d: %v", epoch, err)
	}
	var firstBlock uint64
	if epoch > 0 {
		firstBlock, err = f.getLastBlockOfMap(firstMap - 1)
		if err != nil {
			return false, fmt.Errorf("failed to retrieve last block before deleted epoch %d: %v", epoch, err)
		}
		firstBlock++
	}
	// update rendered range if necessary
	var (
		fmr            = f.indexedRange
		firstEpoch     = f.indexedRange.maps.First() >> f.logMapsPerEpoch
		afterLastEpoch = (f.indexedRange.maps.AfterLast() + f.mapsPerEpoch - 1) >> f.logMapsPerEpoch
	)
	if f.indexedRange.tailPartialEpoch != 0 && firstEpoch > 0 {
		firstEpoch--
	}
	switch {
	case epoch < firstEpoch:
	// cleanup of already unindexed epoch; range not affected
	case epoch == firstEpoch && epoch+1 < afterLastEpoch:
		// first fully or partially rendered epoch and there is at least one
		// rendered map in the next epoch; remove from indexed range
		fmr.tailPartialEpoch = 0
		fmr.maps.SetFirst((epoch + 1) << f.logMapsPerEpoch)
		fmr.blocks.SetFirst(lastBlock + 1)
		batch := f.db.NewBatch()
		err = f.setRange(batch, f.indexedHeight, fmr, false)
		if err != nil {
			return false, err
		}
		err = batch.Write()
		if err != nil {
			return false, err
		}
		return true, nil
	default:
		// cannot be cleaned or unindexed; return with error
		return false, errors.New("invalid tail epoch number")
	}
	// remove index data
	deleteFn := func(db dbm.DB, hashScheme bool, stopCb func(bool) bool) error {
		first := f.mapRowIndex(firstMap, 0)
		count := f.mapRowIndex(firstMap+f.mapsPerEpoch, 0) - first
		if err := DeleteFilterMapRows(f.db, common.NewRange(first, count), hashScheme, stopCb); err != nil {
			return err
		}
		for mapIndex := firstMap; mapIndex < firstMap+f.mapsPerEpoch; mapIndex++ {
			f.filterMapCache.Remove(mapIndex)
		}
		delMapRange := common.NewRange(firstMap, f.mapsPerEpoch-1) // keep last entry
		if err := DeleteFilterMapLastBlocks(f.db, delMapRange, hashScheme, stopCb); err != nil {
			return err
		}
		for mapIndex := firstMap; mapIndex < firstMap+f.mapsPerEpoch-1; mapIndex++ {
			f.lastBlockCache.Remove(mapIndex)
		}
		delBlockRange := common.NewRange(firstBlock, lastBlock-firstBlock) // keep last entry
		if err := DeleteBlockLvPointers(f.db, delBlockRange, hashScheme, stopCb); err != nil {
			return err
		}
		for blockNumber := firstBlock; blockNumber < lastBlock; blockNumber++ {
			f.lvPointerCache.Remove(blockNumber)
		}
		return nil
	}
	action := fmt.Sprintf("Deleting tail epoch #%d", epoch)
	stopFn := func() bool {
		f.processEvents()
		return f.stop || !f.targetHeadIndexed()
	}
	if err := f.safeDeleteWithLogs(deleteFn, action, stopFn); err == nil {
		// everything removed; mark as cleaned and report success
		if f.cleanedEpochsBefore == epoch {
			f.cleanedEpochsBefore = epoch + 1
		}
		return true, nil
	} else {
		// more data left in epoch range; mark as dirty and report unfinished
		if f.cleanedEpochsBefore > epoch {
			f.cleanedEpochsBefore = epoch
		}
		return false, err
	}
}
