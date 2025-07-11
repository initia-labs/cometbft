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
	firstEpoch, lastEpoch := m.firstMap>>m.params.logMapsPerEpoch, m.lastMap>>m.params.logMapsPerEpoch

	// Process epochs sequentially to maintain order
	for epochIndex := firstEpoch; epochIndex <= lastEpoch; epochIndex++ {
		if err := m.processEpochStreaming(epochIndex); err != nil {
			if err == ErrMatchAll {
				return err
			}
			return fmt.Errorf("failed to process log index epoch %d: %v", epochIndex, err)
		}
	}

	return nil
}

// processEpochStreaming processes one epoch and streams results to the result channel
func (m *matcherEnv) processEpochStreaming(epochIndex uint32) error {
	// create a list of map indices to process
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

	// Check if this is a multiEventMatcher
	if multiMatcher, ok := m.matcher.(*multiEventMatcher); ok {
		return m.processMultiEventEpochDirectStreaming(multiMatcher, mapIndices)
	}

	// find potential matches for regular matchers
	matches, err := m.getAllMatches(mapIndices)
	if err != nil {
		return err
	}

	// get the actual logs located at the matching log value indices
	var st int
	m.getLogStats.setState(&st, stGetLog)
	defer m.getLogStats.setState(&st, stNone)

	for _, match := range matches {
		if match == nil {
			return ErrMatchAll
		}
		if err := m.streamLogsFromMatches(match); err != nil {
			return err
		}
	}

	return nil
}

// processMultiEventEpochDirectStreaming processes a multiEventMatcher and streams results
// directly to the result channel without collecting all results first
func (m *matcherEnv) processMultiEventEpochDirectStreaming(multiMatcher *multiEventMatcher, mapIndices []uint32) error {
	numMatchers := len(multiMatcher.matchers)
	// if numMatchers == 1 {
	// 	// single matcher optimization
	// 	return m.processSingleMatcherEpochStreaming(multiMatcher.matchers[0], mapIndices)
	// }

	// create channels for each matcher
	channels := make([]<-chan streamingTxEventResult, numMatchers)

	// start each matcher in a separate goroutine
	for i, singleMatcher := range multiMatcher.matchers {
		ch := make(chan streamingTxEventResult, 100) // buffered for performance
		channels[i] = ch

		go m.runStreamingMatcher(singleMatcher, mapIndices, ch)
	}

	// perform streaming intersection and send results directly to result channel
	return m.streamingIntersectTxEventsDirectly(channels)
}

type matcherEnv struct {
	getLogStats           runtimeStats // 64 bit aligned
	ctx                   context.Context
	backend               MatcherBackend
	params                *Params
	matcher               matcher
	firstIndex, lastIndex uint64
	firstMap, lastMap     uint32
	resultCh              chan<- *TxEvent
}

// streamingTxEventResult represents a streaming result from a matcher
type streamingTxEventResult struct {
	txEvent *TxEvent
	err     error
	done    bool // indicates end of stream
}

// runStreamingMatcher runs a single matcher and streams results to a channel
func (m *matcherEnv) runStreamingMatcher(singleMatcher *singleMatcher, mapIndices []uint32, ch chan<- streamingTxEventResult) {
	defer close(ch)

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

	const batchSize = 32
	seenTxs := make(map[txKey]bool)

	for i := 0; i < len(mapIndices); i += batchSize {
		end := i + batchSize
		if end > len(mapIndices) {
			end = len(mapIndices)
		}

		batch := mapIndices[i:end]

		// Process batch and stream results
		if err := m.processBatch(tempEnv, batch, seenTxs, ch); err != nil {
			return
		}
	}

	// signal end of stream
	ch <- streamingTxEventResult{done: true}
}

// processBatch processes a batch of map indices and streams results immediately
func (m *matcherEnv) processBatch(tempEnv *matcherEnv, batch []uint32, seenTxs map[txKey]bool, ch chan<- streamingTxEventResult) error {
	matchResultCh := make(chan matcherStreamingResult, len(batch))
	errCh := make(chan error, 1)

	go func() {
		defer close(matchResultCh)
		if err := tempEnv.streamingGetAllMatches(batch, matchResultCh); err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	for {
		select {
		case result, ok := <-matchResultCh:
			if !ok {
				// Channel closed, batch processing complete
				return nil
			}

			if result.matches == nil {
				ch <- streamingTxEventResult{err: ErrMatchAll}
				return nil
			}

			mTxEvents, err := tempEnv.getLogsFromMatches(result.matches)
			if err != nil {
				ch <- streamingTxEventResult{err: err}
				return nil
			}

			// Stream unique transactions immediately as they're found
			for _, txEvent := range mTxEvents {
				key := txKey{
					blockNumber: uint64(txEvent.BlockNumber),
					txIndex:     uint32(txEvent.TxIndex),
				}

				if !seenTxs[key] {
					seenTxs[key] = true
					// Send immediately without collecting
					select {
					case ch <- streamingTxEventResult{txEvent: txEvent}:
					case <-m.ctx.Done():
						return nil
					}
				}
			}

		case err := <-errCh:
			if err != nil {
				ch <- streamingTxEventResult{err: err}
				return nil
			}

		case <-m.ctx.Done():
			return nil
		}
	}
}

// streamingIntersectTxEventsDirectly performs N-way streaming intersection and sends results directly to result channel
func (m *matcherEnv) streamingIntersectTxEventsDirectly(channels []<-chan streamingTxEventResult) error {
	numChannels := len(channels)
	if numChannels == 0 {
		return nil
	}

	// current head of each stream
	heads := make([]*TxEvent, numChannels)
	done := make([]bool, numChannels)
	errored := false

	// initialize heads by reading first item from each channel
	for i := 0; i < numChannels; i++ {
		if err := m.readNextFromChannel(channels[i], &heads[i], &done[i], &errored); err != nil {
			return err
		}
		if errored {
			return fmt.Errorf("error in matcher %d", i)
		}
	}

	for {
		// check if context is canceled
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		default:
		}

		// check if any stream is done
		anyDone := false
		for i := 0; i < numChannels; i++ {
			if done[i] {
				anyDone = true
				break
			}
		}
		if anyDone {
			break // intersection is complete
		}

		// find minimum and maximum transaction keys
		minKey := txKey{blockNumber: uint64(heads[0].BlockNumber), txIndex: uint32(heads[0].TxIndex)}
		maxKey := minKey

		for i := 1; i < numChannels; i++ {
			key := txKey{blockNumber: uint64(heads[i].BlockNumber), txIndex: uint32(heads[i].TxIndex)}
			if compareTxKeys(key, minKey) < 0 {
				minKey = key
			}
			if compareTxKeys(key, maxKey) > 0 {
				maxKey = key
			}
		}

		// if all heads have the same key, we found an intersection
		if compareTxKeys(minKey, maxKey) == 0 {
			// send result directly to channel
			select {
			case <-m.ctx.Done():
				return m.ctx.Err()
			// case m.resultCh <- combineResults(heads):
			case m.resultCh <- heads[0]:
				// sent successfully
			}

			// advance all streams
			for i := 0; i < numChannels; i++ {
				if err := m.readNextFromChannel(channels[i], &heads[i], &done[i], &errored); err != nil {
					return err
				}
				if errored {
					return fmt.Errorf("error in matcher %d", i)
				}
			}
		} else {
			// advance streams that have the minimum key
			for i := 0; i < numChannels; i++ {
				key := txKey{blockNumber: uint64(heads[i].BlockNumber), txIndex: uint32(heads[i].TxIndex)}
				if compareTxKeys(key, minKey) == 0 {
					if err := m.readNextFromChannel(channels[i], &heads[i], &done[i], &errored); err != nil {
						return err
					}
					if errored {
						return fmt.Errorf("error in matcher %d", i)
					}
				}
			}
		}
	}

	return nil
}

// readNextFromChannel reads the next TxEvent from a channel
func (m *matcherEnv) readNextFromChannel(ch <-chan streamingTxEventResult, head **TxEvent, done *bool, errored *bool) error {
	select {
	case result, ok := <-ch:
		if !ok {
			*done = true
			return nil
		}
		if result.err != nil {
			*errored = true
			return result.err
		}
		if result.done {
			*done = true
			return nil
		}
		*head = result.txEvent
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

// compareTxKeys compares two transaction keys for sorting
// returns -1 if a < b, 0 if a == b, 1 if a > b
func compareTxKeys(a, b txKey) int {
	if a.blockNumber < b.blockNumber {
		return -1
	}
	if a.blockNumber > b.blockNumber {
		return 1
	}
	if a.txIndex < b.txIndex {
		return -1
	}
	if a.txIndex > b.txIndex {
		return 1
	}
	return 0
}

// txKey represents a unique transaction identifier
type txKey struct {
	blockNumber uint64
	txIndex     uint32
}

// streamLogsFromMatches streams potentially matching logs located at
// the given list of matching log indices to the result channel.
// Matches outside the firstIndex to lastIndex range are not sent.
func (m *matcherEnv) streamLogsFromMatches(matches potentialMatches) error {
	for _, match := range matches {
		if match < m.firstIndex || match > m.lastIndex {
			continue
		}
		txEvent, err := m.backend.GetLogByLvIndex(m.ctx, match)
		if err != nil {
			return fmt.Errorf("failed to retrieve log at index %d: %v", match, err)
		}
		if txEvent != nil {
			select {
			case m.resultCh <- txEvent:
				// sent successfully
			case <-m.ctx.Done():
				return m.ctx.Err()
			}
		}
	}
	return nil
}

// getLogsFromMatches returns the list of potentially matching logs located at
// the given list of matching log indices. Matches outside the firstIndex to
// lastIndex range are not returned. This is used for intermediate processing
// where results need to be collected before streaming.
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

// streamingGetAllMatches sends results as they become available instead of waiting for all results
func (m *matcherEnv) streamingGetAllMatches(mapIndices []uint32, resultCh chan<- matcherStreamingResult) error {

	instance := m.matcher.newInstance(mapIndices)
	remainingIndices := len(mapIndices)
	maxLayers := uint32(10) // Reasonable limit to prevent infinite loops

	for layerIndex := uint32(0); remainingIndices > 0 && layerIndex < maxLayers; layerIndex++ {
		results, err := instance.getMatchesForLayer(m.ctx, layerIndex)
		if err != nil {
			return err
		}

		// Stream results as they become available
		for _, result := range results {
			select {
			case resultCh <- matcherStreamingResult{
				mapIndex: result.mapIndex,
				matches:  result.matches,
			}:
				remainingIndices--
			case <-m.ctx.Done():
				return m.ctx.Err()
			}
		}

		// Context check between layers
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		default:
		}
	}

	return nil
}

type matcherStreamingResult struct {
	mapIndex uint32
	matches  potentialMatches
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
