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
	"context"
	"errors"
	"fmt"
	"sync"
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

// GetPotentialMatches returns a list of logs that are potential matches for the
// given filter criteria. This finds logs that contain ALL of the specified events
// by processing multiple singleMatchers in parallel and computing intersection at TxEvent level.
func GetPotentialMatches(ctx context.Context, logger log.Logger, backend MatcherBackend, firstBlock, lastBlock uint64, events []string) ([]*TxEvent, error) {
	params := backend.GetParams()
	// find the log value index range to search
	firstIndex, err := backend.GetBlockLvPointer(ctx, firstBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve log value pointer for first block %d: %v", firstBlock, err)
	}
	lastIndex, err := backend.GetBlockLvPointer(ctx, lastBlock+1)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve log value pointer after last block %d: %v", lastBlock, err)
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
	}

	res, err := m.process()
	return res, err
}

type matcherEnv struct {
	getLogStats           runtimeStats // 64 bit aligned
	ctx                   context.Context
	backend               MatcherBackend
	params                *Params
	matcher               matcher
	firstIndex, lastIndex uint64
	firstMap, lastMap     uint32
}

func (m *matcherEnv) process() ([]*TxEvent, error) {
	type task struct {
		epochIndex uint32
		txEvents   []*TxEvent
		err        error
		done       chan struct{}
	}

	taskCh := make(chan *task)
	var wg sync.WaitGroup
	defer func() {
		close(taskCh)
		wg.Wait()
	}()

	worker := func() {
		for task := range taskCh {
			if task == nil {
				break
			}
			task.txEvents, task.err = m.processEpoch(task.epochIndex)
			close(task.done)
		}
		wg.Done()
	}

	for range 4 {
		wg.Add(1)
		go worker()
	}

	firstEpoch, lastEpoch := m.firstMap>>m.params.logMapsPerEpoch, m.lastMap>>m.params.logMapsPerEpoch
	var txEvents []*TxEvent
	// startEpoch is the next task to send whenever a worker can accept it.
	// waitEpoch is the next task we are waiting for to finish in order to append
	// results in the correct order.
	startEpoch, waitEpoch := firstEpoch, firstEpoch
	tasks := make(map[uint32]*task)
	tasks[startEpoch] = &task{epochIndex: startEpoch, done: make(chan struct{})}
	for waitEpoch <= lastEpoch {
		select {
		case taskCh <- tasks[startEpoch]:
			startEpoch++
			if startEpoch <= lastEpoch {
				if tasks[startEpoch] == nil {
					tasks[startEpoch] = &task{epochIndex: startEpoch, done: make(chan struct{})}
				}
			}
		case <-tasks[waitEpoch].done:
			txEvents = append(txEvents, tasks[waitEpoch].txEvents...)
			if err := tasks[waitEpoch].err; err != nil {
				if err == ErrMatchAll {
					return txEvents, err
				}
				return txEvents, fmt.Errorf("failed to process log index epoch %d: %v", waitEpoch, err)
			}
			delete(tasks, waitEpoch)
			waitEpoch++
			if waitEpoch <= lastEpoch {
				if tasks[waitEpoch] == nil {
					tasks[waitEpoch] = &task{epochIndex: waitEpoch, done: make(chan struct{})}
				}
			}
		}
	}
	return txEvents, nil
}

// processEpoch returns the potentially matching logs from the given epoch.
func (m *matcherEnv) processEpoch(epochIndex uint32) ([]*TxEvent, error) {
	var txEvents []*TxEvent
	// create a list of map indices to process
	fm, lm := epochIndex<<m.params.logMapsPerEpoch, (epochIndex+1)<<m.params.logMapsPerEpoch-1
	if fm < m.firstMap {
		fm = m.firstMap
	}
	if lm > m.lastMap {
		lm = m.lastMap
	}
	//
	mapIndices := make([]uint32, lm+1-fm)
	for i := range mapIndices {
		mapIndices[i] = fm + uint32(i)
	}

	// Check if this is a multiEventMatcher
	if multiMatcher, ok := m.matcher.(*multiEventMatcher); ok {
		return m.processMultiEventEpoch(multiMatcher, mapIndices)
	}

	// find potential matches for regular matchers
	matches, err := m.getAllMatches(mapIndices)
	if err != nil {
		return txEvents, err
	}
	// get the actual logs located at the matching log value indices
	var st int
	m.getLogStats.setState(&st, stGetLog)
	defer m.getLogStats.setState(&st, stNone)
	for _, match := range matches {
		if match == nil {
			return nil, ErrMatchAll
		}
		mTxEvents, err := m.getLogsFromMatches(match)
		if err != nil {
			return txEvents, err
		}
		txEvents = append(txEvents, mTxEvents...)
	}
	m.getLogStats.addAmount(st, int64(len(txEvents)))
	return txEvents, nil
}

// processMultiEventEpoch processes a multiEventMatcher by running each individual
// matcher separately and computing intersection at TxEvent level based on
// block number and tx index.
func (m *matcherEnv) processMultiEventEpoch(multiMatcher *multiEventMatcher, mapIndices []uint32) ([]*TxEvent, error) {
	var st int
	m.getLogStats.setState(&st, stGetLog)
	defer m.getLogStats.setState(&st, stNone)

	// collect TxEvents from all individual matchers
	allTxEvents := make([][]*TxEvent, len(multiMatcher.matchers))

	for i, singleMatcher := range multiMatcher.matchers {
		// create a temporary matcherEnv for this single matcher
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

		// get matches for this single matcher
		matches, err := tempEnv.getAllMatches(mapIndices)
		if err != nil {
			return nil, fmt.Errorf("failed to get matches for event %d: %v", i, err)
		}

		// convert potential matches to TxEvents and deduplicate by transaction
		var txEvents []*TxEvent
		seenTxs := make(map[txKey]bool)

		for _, match := range matches {
			if match == nil {
				return nil, ErrMatchAll
			}
			mTxEvents, err := tempEnv.getLogsFromMatches(match)
			if err != nil {
				return nil, fmt.Errorf("failed to get logs for event %d: %v", i, err)
			}

			// deduplicate by transaction (blockNumber, txIndex)
			for _, txEvent := range mTxEvents {
				key := txKey{
					blockNumber: uint64(txEvent.BlockNumber),
					txIndex:     uint32(txEvent.TxIndex),
				}
				if !seenTxs[key] {
					txEvents = append(txEvents, txEvent)
					seenTxs[key] = true
				}
			}
		}

		allTxEvents[i] = txEvents
	}

	// compute intersection at TxEvent level
	intersectionTxEvents := m.computeTxEventIntersection(allTxEvents)

	m.getLogStats.addAmount(st, int64(len(intersectionTxEvents)))
	return intersectionTxEvents, nil
}

// txKey represents a unique transaction identifier
type txKey struct {
	blockNumber uint64
	txIndex     uint32
}

// computeTxEventIntersection computes the intersection of multiple TxEvent slices
// based on block number and tx index.
func (m *matcherEnv) computeTxEventIntersection(allTxEvents [][]*TxEvent) []*TxEvent {
	if len(allTxEvents) == 0 {
		return []*TxEvent{}
	}

	if len(allTxEvents) == 1 {
		return allTxEvents[0]
	}

	// create a map to track (blockNumber, txIndex) occurrences

	// count occurrences of each (blockNumber, txIndex) across all matcher results
	keyCount := make(map[txKey]int)
	keyToTxEvent := make(map[txKey]*TxEvent)

	for _, txEvents := range allTxEvents {
		seenKeys := make(map[txKey]bool) // to avoid double counting within same matcher

		for _, txEvent := range txEvents {
			key := txKey{
				blockNumber: uint64(txEvent.BlockNumber),
				txIndex:     uint32(txEvent.TxIndex),
			}

			// Only count each (blockNumber, txIndex) combination once per matcher
			// This handles the case where same transaction has multiple events of same type
			if !seenKeys[key] {
				keyCount[key]++
				// Always store the first TxEvent we encounter for this key
				// (we only care about the transaction, not the specific event within it)
				if keyToTxEvent[key] == nil {
					keyToTxEvent[key] = txEvent
				}
				seenKeys[key] = true
			}
		}
	}

	// collect TxEvents that appear in all matcher results
	var result []*TxEvent
	requiredCount := len(allTxEvents)

	for key, count := range keyCount {
		if count == requiredCount {
			result = append(result, keyToTxEvent[key])
		}
	}

	return result
}

// getLogsFromMatches returns the list of potentially matching logs located at
// the given list of matching log indices. Matches outside the firstIndex to
// lastIndex range are not returned.
func (m *matcherEnv) getLogsFromMatches(matches potentialMatches) ([]*TxEvent, error) {
	var txEvents []*TxEvent
	for _, match := range matches {
		if match < m.firstIndex || match > m.lastIndex {
			continue
		}
		txEvent, err := m.backend.GetLogByLvIndex(m.ctx, match)
		if err != nil {
			return txEvents, fmt.Errorf("failed to retrieve log at index %d: %v", match, err)
		}
		if txEvent != nil {
			txEvents = append(txEvents, txEvent)
		}
	}
	return txEvents, nil
}

// getAllMatches creates an instance for a given matcher and set of map indices,
// iterates through mapping layers and collects all results, then returns all
// results in the same order as the map indices were specified.
func (m *matcherEnv) getAllMatches(mapIndices []uint32) ([]potentialMatches, error) {
	instance := m.matcher.newInstance(mapIndices)
	resultsMap := make(map[uint32]potentialMatches)
	for layerIndex := uint32(0); len(resultsMap) < len(mapIndices); layerIndex++ {
		results, err := instance.getMatchesForLayer(m.ctx, layerIndex)
		if err != nil {
			return nil, err
		}
		for _, result := range results {
			resultsMap[result.mapIndex] = result.matches
		}
	}
	matches := make([]potentialMatches, len(mapIndices))
	for i, mapIndex := range mapIndices {
		matches[i] = resultsMap[mapIndex]
	}
	return matches, nil
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
	getMatchesForLayer(ctx context.Context, layerIndex uint32) ([]matcherResult, error)
	dropIndices(mapIndices []uint32)
}

// matcherResult contains the list of potentially matching log value indices
// for a given map index.
type matcherResult struct {
	mapIndex uint32
	matches  potentialMatches
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
func (m *singleMatcherInstance) getMatchesForLayer(ctx context.Context, layerIndex uint32) (results []matcherResult, err error) {
	var st int
	m.stats.setState(&st, stOther)
	params := m.backend.GetParams()
	var ptr int
	for len(m.mapIndices) > ptr {
		// find next group of map indices mapped onto the same row
		maskedMapIndex := params.maskedMapIndex(m.mapIndices[ptr], layerIndex)
		rowIndex := params.rowIndex(m.mapIndices[ptr], layerIndex, m.value)
		groupLength := 1
		for ptr+groupLength < len(m.mapIndices) && params.maskedMapIndex(m.mapIndices[ptr+groupLength], layerIndex) == maskedMapIndex {
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
				results = append(results, matcherResult{
					mapIndex: mapIndex,
					matches:  matches,
				})
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

// matchOrderStats collects statistics about the evaluating cost and the
// occurrence of empty result sets from both base and next child matchers.
// This allows the optimization of the evaluation order by evaluating the
// child first that is cheaper and/or gives empty results more often and not
// evaluating the other child in most cases.
// Note that matchOrderStats is specific to matchSequence and the results are
// carried over to future instances as the results are mostly useful when
// evaluating layer zero of each instance. For this reason it should be used
// in a thread safe way as is may be accessed from multiple worker goroutines.
type matchOrderStats struct {
	totalCount, nonEmptyCount, totalCost uint64
}

// add collects statistics after a child has been evaluated for a certain layer.
func (ms *matchOrderStats) add(empty bool, layerIndex uint32) {
	if empty && layerIndex != 0 {
		// matchers may be evaluated for higher layers after all results have
		// been returned. Also, empty results are not relevant when previous
		// layers yielded matches already, so these cases can be ignored.
		return
	}
	ms.totalCount++
	if !empty {
		ms.nonEmptyCount++
	}
	ms.totalCost += uint64(layerIndex + 1)
}

// mergeStats merges two sets of matchOrderStats.
func (ms *matchOrderStats) mergeStats(add matchOrderStats) {
	ms.totalCount += add.totalCount
	ms.nonEmptyCount += add.nonEmptyCount
	ms.totalCost += add.totalCost
}

// matchResults returns a list of sequence matches for the given mapIndex and
// offset based on the base matcher's results at mapIndex and the next matcher's
// results at mapIndex and mapIndex+1. Note that acquiring nextNextRes may be
// skipped and it can be substituted with an empty list if baseRes has no potential
// matches that could be sequence matched with anything that could be in nextNextRes.
func (params *Params) matchResults(mapIndex uint32, offset uint64, baseRes, nextRes potentialMatches) potentialMatches {
	if nextRes == nil || (baseRes != nil && len(baseRes) == 0) {
		// if nextRes is a wild card or baseRes is empty then the sequence matcher
		// result equals baseRes.
		return baseRes
	}
	if baseRes == nil || len(nextRes) == 0 {
		// if baseRes is a wild card or nextRes is empty then the sequence matcher
		// result is the items of nextRes with a negative offset applied.
		result := make(potentialMatches, 0, len(nextRes))
		min := (uint64(mapIndex) << params.logValuesPerMap) + offset
		for _, v := range nextRes {
			if v >= min {
				result = append(result, v-offset)
			}
		}
		return result
	}
	// iterate through baseRes and nextRes in parallel and collect matching results.
	maxLen := len(baseRes)
	if l := len(nextRes); l < maxLen {
		maxLen = l
	}
	matchedRes := make(potentialMatches, 0, maxLen)
	for len(nextRes) > 0 && len(baseRes) > 0 {
		if nextRes[0] > baseRes[0]+offset {
			baseRes = baseRes[1:]
		} else if nextRes[0] < baseRes[0]+offset {
			nextRes = nextRes[1:]
		} else {
			matchedRes = append(matchedRes, baseRes[0])
			baseRes = baseRes[1:]
			nextRes = nextRes[1:]
		}
	}
	return matchedRes
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
func (m *multiEventMatcherInstance) getMatchesForLayer(ctx context.Context, layerIndex uint32) ([]matcherResult, error) {
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
