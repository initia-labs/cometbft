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
	"container/list"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"

	"github.com/cometbft/cometbft/libs/log"
)

const doRuntimeStats = true

// ErrMatchAll is returned when the specified filter matches everything.
// Handling this case in filtermaps would require an extra special case and
// would actually be slower than reverting to legacy filter.
var ErrMatchAll = errors.New("match all patterns not supported")

// MatcherBackend defines the functions required for searching in the log index
// data structure. It is currently implemented by FilterMapsMatcherBackend but
// once EIP-7745 is implemented and active, these functions can also be trustlessly
// served by a remote prover.
type MatcherBackend interface {
	GetParams() *Params
	GetBlockLvPointer(ctx context.Context, blockNumber uint64) (uint64, error)
	GetFilterMapRows(ctx context.Context, mapIndices []uint32, rowIndex uint32, baseLayerOnly bool) ([]FilterRow, error)
	GetLogByLvIndex(ctx context.Context, lvIndex uint64) (*TxEvent, error)
	GetLvIndexRange(ctx context.Context, lvIndex uint64) (lvIndexRange, error)
	SyncLogIndex(ctx context.Context) (SyncRange, error)
	Close()
}

// SyncRange is returned by MatcherBackend.SyncLogIndex. It contains the latest
// chain head, the indexed range that is currently consistent with the chain
// and the valid range that has not been changed and has been consistent with
// all states of the chain since the previous SyncLogIndex or the creation of
// the matcher backend.
type SyncRange struct {
	IndexedHeight uint64
	// block range where the index has not changed since the last matcher sync
	// and therefore the set of matches found in this region is guaranteed to
	// be valid and complete.
	ValidBlocks common.Range[uint64]
	// block range indexed according to the given chain head.
	IndexedBlocks common.Range[uint64]
}

// GetPotentialMatches streams logs that are potential matches for the
// given filter criteria. This finds logs that contain ALL of the specified events
// by processing multiple singleMatchers in parallel and computing intersection at TxEvent level.
// Results are sent to the provided channel one by one. The function returns when
// context is canceled or all results are processed.
func GetPotentialMatches(ctx context.Context, logger log.Logger, backend MatcherBackend, firstBlock, lastBlock uint64, events []string, resultCh chan<- *TxEvent) error {
	params := backend.GetParams()
	// find the log value index range to search
	firstIndex, err := backend.GetBlockLvPointer(ctx, firstBlock)
	if err != nil {
		return fmt.Errorf("failed to retrieve log value pointer for first block %d: %v", firstBlock, err)
	}
	lastIndex, err := backend.GetBlockLvPointer(ctx, lastBlock+1)
	if err != nil {
		return fmt.Errorf("failed to retrieve log value pointer after last block %d: %v", lastBlock, err)
	}
	if lastIndex > 0 {
		lastIndex--
	}

	// create multiple singleMatcher instances
	matchers := make([]*singleMatcher, len(events))
	for i, event := range events {
		matchers[i] = &singleMatcher{
			backend: backend,
			value:   eventValue(event),
		}
	}

	// create multi-event matcher that processes all singleMatchers in parallel
	matcher := newMultiEventMatcher(matchers)

	m := &matcherEnv{
		ctx:        ctx,
		backend:    backend,
		params:     params,
		matcher:    matcher,
		firstIndex: firstIndex,
		lastIndex:  lastIndex,
		firstMap:   uint32(firstIndex >> params.logValuesPerMap),
		lastMap:    uint32(lastIndex >> params.logValuesPerMap),
		resultCh:   resultCh,
	}

	return m.processStreaming()
}

// processStreaming processes the matcher and streams results to the result channel
// Processes epochs sequentially to guarantee ordering by (blockNumber, txIndex)
func (m *matcherEnv) processStreaming() error {
	multiMatcher, ok := m.matcher.(*multiEventMatcher)
	if !ok {
		return errors.New("not a multiEventMatcher")
	}

	firstEpoch, lastEpoch := m.firstMap>>m.params.logMapsPerEpoch, m.lastMap>>m.params.logMapsPerEpoch

	// Process epochs sequentially to maintain order
	for epochIndex := firstEpoch; epochIndex <= lastEpoch; epochIndex++ {
		fm, lm := epochIndex<<m.params.logMapsPerEpoch, (epochIndex+1)<<m.params.logMapsPerEpoch-1
		if fm < m.firstMap {
			fm = m.firstMap
		}
		if lm > m.lastMap {
			lm = m.lastMap
		}

		mapIndices := make([]uint32, lm+1-fm)
		for i := range mapIndices {
			mapIndices[i] = fm + uint32(i)
		}

		for i := 0; i < len(mapIndices); i += batchSize {
			end := i + batchSize
			if end > len(mapIndices) {
				end = len(mapIndices)
			}

			batch := mapIndices[i:end]

			err := m.runMatcher(multiMatcher.matchers, batch)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type matcherEnv struct {
	ctx                   context.Context
	backend               MatcherBackend
	params                *Params
	matcher               matcher
	firstIndex, lastIndex uint64
	firstMap, lastMap     uint32
	resultCh              chan<- *TxEvent
}

const batchSize = 32

type singleMatcherResult struct {
	index   int
	matches potentialMatches
}

type potentialResult struct {
	match        uint64
	lvIndexRange *lvIndexRange
	txIndex      uint64
}

// runStreamingMatcher runs a single matcher and streams results to a channel
func (m *matcherEnv) runMatcher(matchers []*singleMatcher, batch []uint32) error {
	matcherResults := make([]singleMatcherResult, len(matchers))

	for i, singleMatcher := range matchers {
		// create temporary matcherEnv for this single matcher
		tempEnv := &matcherEnv{
			ctx:        m.ctx,
			backend:    m.backend,
			params:     m.params,
			matcher:    singleMatcher,
			firstIndex: m.firstIndex,
			lastIndex:  m.lastIndex,
			firstMap:   m.firstMap,
			lastMap:    m.lastMap,
		}

		results, err := tempEnv.getAllMatches(batch)
		if err != nil {
			return err
		}
		matcherResults[i] = singleMatcherResult{
			index:   i,
			matches: results,
		}
	}

	// sort by number of matches
	sort.Slice(matcherResults, func(i, j int) bool {
		return len(matcherResults[i].matches) < len(matcherResults[j].matches)
	})

	potentialResults, err := m.getPotentialResults(&matcherResults[0])
	if err != nil {
		return err
	}
	for elem := potentialResults.Front(); elem != nil; elem = elem.Next() {
	}

	for _, matcherResult := range matcherResults[1:] {
		err := m.deleteUnmatchedResults(potentialResults, &matcherResult)
		if err != nil {
			return err
		}
	}

	result := potentialResults.Front()
	for result != nil {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case m.resultCh <- &TxEvent{
			BlockNumber: int64(result.Value.(potentialResult).lvIndexRange.blockNumber + 1),
			TxIndex:     int(result.Value.(potentialResult).txIndex),
		}:
			result = result.Next()
		}
	}
	return nil
}

func (m *matcherEnv) getPotentialResults(matcherResult *singleMatcherResult) (*list.List, error) {
	potentialResults := list.New()

	var rg *lvIndexRange
	for _, result := range matcherResult.matches {
		if !isLvIndexInRange(result, rg) {
			lvIndexRange, err := m.backend.GetLvIndexRange(m.ctx, result)
			if err != nil {
				return nil, err
			}
			rg = &lvIndexRange
		}

		txIndex, err := getTxIndexInRange(result, rg)
		if err != nil {
			return nil, err
		}

		if lastElem := potentialResults.Back(); lastElem == nil ||
			lastElem.Value.(potentialResult).txIndex != txIndex ||
			lastElem.Value.(potentialResult).lvIndexRange.blockNumber != rg.blockNumber {
			potentialResults.PushBack(potentialResult{
				match:        result,
				lvIndexRange: rg,
				txIndex:      txIndex,
			})
		}
	}
	return potentialResults, nil
}

func (m *matcherEnv) deleteUnmatchedResults(potentialResults *list.List, matcherResult *singleMatcherResult) error {
	currentPotentialResult := potentialResults.Front()
	matcherResultPointer := 0
	for matcherResultPointer < len(matcherResult.matches) && currentPotentialResult != nil {
		potentialResult := currentPotentialResult.Value.(potentialResult)

		lr, err := lvIndexInRange(matcherResult.matches[matcherResultPointer], potentialResult.lvIndexRange)
		if err != nil {
			return err
		} else if lr == 0 {
			currentPotentialResult = currentPotentialResult.Next()
			matcherResultPointer++
		} else if lr < 0 {
			matcherResultPointer++
		} else {
			nextElem := currentPotentialResult.Next()
			potentialResults.Remove(currentPotentialResult)
			currentPotentialResult = nextElem
		}
	}
	return nil
}

func isLvIndexInRange(lvIndex uint64, rg *lvIndexRange) bool {
	if rg == nil {
		return false
	}
	return lvIndex >= rg.startLvIndex && lvIndex < rg.startLvIndex+rg.accumulatedPointers[len(rg.accumulatedPointers)-1]
}

func lvIndexInRange(lvIndex uint64, rg *lvIndexRange) (int, error) {
	if rg == nil {
		return 0, fmt.Errorf("lvIndexRange is nil")
	} else if lvIndex < rg.startLvIndex {
		return -1, nil
	} else if lvIndex >= rg.startLvIndex+rg.accumulatedPointers[len(rg.accumulatedPointers)-1] {
		return 1, nil
	} else {
		return 0, nil
	}
}

func getTxIndexInRange(lvIndex uint64, rg *lvIndexRange) (uint64, error) {
	startLvIndex := rg.startLvIndex
	for i, accumulatedPointer := range rg.accumulatedPointers {
		if lvIndex >= startLvIndex && lvIndex < startLvIndex+accumulatedPointer {
			return uint64(i), nil
		}
	}
	return 0, fmt.Errorf("log value index %d is not in range %d-%d", lvIndex, rg.startLvIndex, rg.startLvIndex+rg.accumulatedPointers[len(rg.accumulatedPointers)-1])
}

func (m *matcherEnv) getAllMatches(mapIndices []uint32) (potentialMatches, error) {
	matcherResults := make(potentialMatches, 0)
	instance := m.matcher.newInstance(mapIndices)
	remainingIndices := len(mapIndices)
	maxLayers := uint32(10) // Reasonable limit to prevent infinite loops

	for layerIndex := uint32(0); remainingIndices > 0 && layerIndex < maxLayers; layerIndex++ {
		results, err := instance.getMatchesForLayer(m.ctx, layerIndex)
		if err != nil {
			return nil, err
		}
		matcherResults = append(matcherResults, results...)

		// Context check between layers
		select {
		case <-m.ctx.Done():
			return nil, m.ctx.Err()
		default:
		}
	}
	return matcherResults, nil
}

// matcher defines a general abstraction for any matcher configuration that
// can instantiate a matcherInstance.
type matcher interface {
	newInstance(mapIndices []uint32) matcherInstance
}

// matcherInstance defines a general abstraction for a matcher configuration
// working on a specific set of map indices and eventually returning a list of
// potentially matching log value indices.
// Note that processing happens per mapping layer, each call returning a set
// of results for the maps where the processing has been finished at the given
// layer. Map indices can also be dropped before a result is returned for them
// in case the result is no longer interesting. Dropping indices twice or after
// a result has been returned has no effect. Exactly one matcherResult is
// returned per requested map index unless dropped.
type matcherInstance interface {
	getMatchesForLayer(ctx context.Context, layerIndex uint32) (potentialMatches, error)
	dropIndices(mapIndices []uint32)
}

// singleMatcher implements matcher by returning matches for a single log value hash.
type singleMatcher struct {
	backend MatcherBackend
	value   common.Hash
	stats   runtimeStats
}

// singleMatcherInstance is an instance of singleMatcher.
type singleMatcherInstance struct {
	*singleMatcher
	mapIndices []uint32
	filterRows map[uint32][]FilterRow
}

// newInstance creates a new instance of singleMatcher.
func (m *singleMatcher) newInstance(mapIndices []uint32) matcherInstance {
	filterRows := make(map[uint32][]FilterRow)
	for _, idx := range mapIndices {
		filterRows[idx] = []FilterRow{}
	}
	copiedIndices := make([]uint32, len(mapIndices))
	copy(copiedIndices, mapIndices)
	return &singleMatcherInstance{
		singleMatcher: m,
		mapIndices:    copiedIndices,
		filterRows:    filterRows,
	}
}

// getMatchesForLayer implements matcherInstance.
func (m *singleMatcherInstance) getMatchesForLayer(ctx context.Context, layerIndex uint32) (results potentialMatches, err error) {
	var st int
	m.stats.setState(&st, stOther)
	params := m.backend.GetParams()
	var ptr int
	for len(m.mapIndices) > ptr {
		// Check context for early termination
		select {
		case <-ctx.Done():
			m.stats.setState(&st, stNone)
			return nil, ctx.Err()
		default:
		}

		// find next group of map indices mapped onto the same row
		maskedMapIndex := params.maskedMapIndex(m.mapIndices[ptr], layerIndex)
		rowIndex := params.rowIndex(m.mapIndices[ptr], layerIndex, m.value)
		groupLength := 1
		maxGroupSize := 32
		if len(m.mapIndices)-ptr < maxGroupSize {
			maxGroupSize = len(m.mapIndices) - ptr
		}
		for ptr+groupLength < len(m.mapIndices) && groupLength < maxGroupSize && params.maskedMapIndex(m.mapIndices[ptr+groupLength], layerIndex) == maskedMapIndex {
			groupLength++
		}
		if layerIndex == 0 {
			m.stats.setState(&st, stFetchFirst)
		} else {
			m.stats.setState(&st, stFetchMore)
		}
		groupRows, err := m.backend.GetFilterMapRows(ctx, m.mapIndices[ptr:ptr+groupLength], rowIndex, layerIndex == 0)
		if err != nil {
			m.stats.setState(&st, stNone)
			return nil, fmt.Errorf("failed to retrieve filter map %d row %d: %v", m.mapIndices[ptr], rowIndex, err)
		}
		m.stats.setState(&st, stOther)
		for i := range groupLength {
			mapIndex := m.mapIndices[ptr+i]
			filterRow := groupRows[i]
			filterRows, ok := m.filterRows[mapIndex]
			if !ok {
				panic("dropped map in mapIndices")
			}
			m.stats.addAmount(st, int64(len(filterRow)))
			filterRows = append(filterRows, filterRow)
			if uint32(len(filterRow)) < params.maxRowLength(layerIndex) {
				m.stats.setState(&st, stProcess)
				matches := params.potentialMatches(filterRows, mapIndex, m.value)
				m.stats.addAmount(st, int64(len(matches)))
				results = append(results, matches...)
				m.stats.setState(&st, stOther)
				delete(m.filterRows, mapIndex)
			} else {
				m.filterRows[mapIndex] = filterRows
			}
		}
		ptr += groupLength
	}
	m.cleanMapIndices()
	m.stats.setState(&st, stNone)
	return results, nil
}

// dropIndices implements matcherInstance.
func (m *singleMatcherInstance) dropIndices(dropIndices []uint32) {
	for _, mapIndex := range dropIndices {
		delete(m.filterRows, mapIndex)
	}
	m.cleanMapIndices()
}

// cleanMapIndices removes map indices from the list if there is no matching
// filterRows entry because a result has been returned or the index has been
// dropped.
func (m *singleMatcherInstance) cleanMapIndices() {
	var j int
	for i, mapIndex := range m.mapIndices {
		if _, ok := m.filterRows[mapIndex]; ok {
			if i != j {
				m.mapIndices[j] = mapIndex
			}
			j++
		}
	}
	m.mapIndices = m.mapIndices[:j]
}

// runtimeStats collects processing time statistics while searching in the log
// index. Used only when the doRuntimeStats global flag is true.
type runtimeStats struct {
	dt, cnt, amount [stCount]int64
}

const (
	stNone = iota
	stFetchFirst
	stFetchMore
	stProcess
	stGetLog
	stOther
	stCount
)

var stNames = []string{"", "fetchFirst", "fetchMore", "process", "getLog", "other"}

// set sets the processing state to one of the pre-defined constants.
// Processing time spent in each state is measured separately.
func (ts *runtimeStats) setState(state *int, newState int) {
	if !doRuntimeStats || newState == *state {
		return
	}
	now := int64(mclock.Now())
	atomic.AddInt64(&ts.dt[*state], now)
	atomic.AddInt64(&ts.dt[newState], -now)
	atomic.AddInt64(&ts.cnt[newState], 1)
	*state = newState
}

func (ts *runtimeStats) addAmount(state int, amount int64) {
	atomic.AddInt64(&ts.amount[state], amount)
}

// print prints the collected statistics.
func (ts *runtimeStats) print(logger log.Logger) {
	for i := 1; i < stCount; i++ {
		logger.Info("Matcher stats", "name", stNames[i], "dt", time.Duration(ts.dt[i]), "count", ts.cnt[i], "amount", ts.amount[i])
	}
}

// multiEventMatcher processes multiple singleMatcher instances and
// computes intersection at TxEvent level based on block number and tx index.
type multiEventMatcher struct {
	matchers []*singleMatcher
}

// newMultiEventMatcher creates a new multiEventMatcher from multiple singleMatcher instances.
func newMultiEventMatcher(matchers []*singleMatcher) matcher {
	return &multiEventMatcher{
		matchers: matchers,
	}
}

// newInstance creates a new instance of multiEventMatcher.
func (m *multiEventMatcher) newInstance(mapIndices []uint32) matcherInstance {
	if len(m.matchers) == 0 {
		panic("multiEventMatcher cannot have zero matchers")
	}

	// For multiple matchers, we need special handling
	// Return a special instance that will be handled differently in processEpoch
	return &multiEventMatcherInstance{
		multiEventMatcher: m,
		mapIndices:        append([]uint32(nil), mapIndices...),
	}
}

// multiEventMatcherInstance is an instance of multiEventMatcher.
type multiEventMatcherInstance struct {
	*multiEventMatcher
	mapIndices []uint32
}

// getMatchesForLayer implements matcherInstance.
// This is a placeholder - the actual processing happens in processEpoch.
func (m *multiEventMatcherInstance) getMatchesForLayer(ctx context.Context, layerIndex uint32) (potentialMatches, error) {
	// This should not be called for multi-event matching
	// The actual logic is in processEpoch
	return nil, fmt.Errorf("multiEventMatcherInstance.getMatchesForLayer should not be called")
}

// dropIndices implements matcherInstance.
func (m *multiEventMatcherInstance) dropIndices(dropIndices []uint32) {
	if len(dropIndices) == 0 {
		return
	}

	keep := m.mapIndices[:0]
	dropSet := make(map[uint32]struct{}, len(dropIndices))
	for _, idx := range dropIndices {
		dropSet[idx] = struct{}{}
	}

	for _, idx := range m.mapIndices {
		if _, shouldDrop := dropSet[idx]; !shouldDrop {
			keep = append(keep, idx)
		}
	}
	m.mapIndices = keep
}
