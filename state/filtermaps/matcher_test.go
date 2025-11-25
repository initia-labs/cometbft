package filtermaps

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

type stubMatcherBackend struct {
	params  *Params
	rows    map[uint32]FilterRow
	lvRange lvIndexRange
}

func (s *stubMatcherBackend) GetParams() *Params { return s.params }

func (s *stubMatcherBackend) GetBlockLvPointer(context.Context, uint64) (uint64, error) {
	return 0, errors.New("unexpected GetBlockLvPointer call")
}

func (s *stubMatcherBackend) GetFilterMapRows(_ context.Context, mapIndices []uint32, _ uint32, _ bool) ([]FilterRow, error) {
	rows := make([]FilterRow, len(mapIndices))
	for i, idx := range mapIndices {
		if row, ok := s.rows[idx]; ok {
			rows[i] = row
		}
	}
	return rows, nil
}

func (s *stubMatcherBackend) GetLogByLvIndex(context.Context, uint64) (*TxEvent, error) {
	return nil, errors.New("unexpected GetLogByLvIndex call")
}

func (s *stubMatcherBackend) GetLvIndexRange(context.Context, uint64) (lvIndexRange, error) {
	return s.lvRange, nil
}

func (s *stubMatcherBackend) SyncLogIndex(context.Context) (SyncRange, error) {
	return SyncRange{}, errors.New("unexpected SyncLogIndex call")
}

func (s *stubMatcherBackend) Close() {}

// Ensures the matcher no longer aborts when a matcher returns zero results.
func TestRunMatcherAllowsZeroMatches(t *testing.T) {
	params := Params{
		logMapHeight:       3,
		logMapWidth:        4,
		logMapsPerEpoch:    1,
		logValuesPerMap:    4,
		baseRowGroupLength: 4,
		baseRowLengthRatio: 2,
		logLayerDiff:       2,
	}
	params.deriveFields()

	backend := &stubMatcherBackend{
		params: &params,
		lvRange: lvIndexRange{
			blockNumber:  0,
			startLvIndex: 0,
			lvPointers:   []uint64{0, params.valuesPerMap},
		},
		rows: make(map[uint32]FilterRow),
	}
	matchers := []*singleMatcher{
		{backend: backend, value: common.Hash{1}},
		{backend: backend, value: common.Hash{2}},
	}

	resultCh := make(chan *TxEvent, 1)

	env := &matcherEnv{
		ctx:        context.Background(),
		backend:    backend,
		params:     &params,
		matcher:    newMultiEventMatcher(matchers),
		firstMap:   0,
		lastMap:    0,
		resultCh:   resultCh,
		firstIndex: 0,
		lastIndex:  0,
	}

	if err := env.runMatcher(matchers, []uint32{0}); err != nil {
		t.Fatalf("runMatcher returned error for zero matches: %v", err)
	}

	select {
	case res := <-resultCh:
		t.Fatalf("expected no results, got %+v", res)
	default:
	}
}

// Ensures mixed zero/non-zero matchers don't error and still produce matches
// when at least one matcher yields results for the batch.
func TestRunMatcherMixedZeroAndNonZeroMatches(t *testing.T) {
	params := Params{
		logMapHeight:       3,
		logMapWidth:        4,
		logMapsPerEpoch:    1,
		logValuesPerMap:    4,
		baseRowGroupLength: 4,
		baseRowLengthRatio: 2,
		logLayerDiff:       2,
	}
	params.deriveFields()

	valueWithMatch := common.Hash{3}
	mapIndex := uint32(0)
	colIndex := params.columnIndex(0, &valueWithMatch)

	backend := &stubMatcherBackend{
		params: &params,
		rows: map[uint32]FilterRow{
			mapIndex: {colIndex},
		},
		lvRange: lvIndexRange{
			blockNumber:  0,
			startLvIndex: 0,
			lvPointers:   []uint64{0, params.valuesPerMap},
		},
	}

	matchers := []*singleMatcher{
		{backend: backend, value: common.Hash{9}}, // zero matches
		{backend: backend, value: valueWithMatch}, // has a match
	}

	resultCh := make(chan *TxEvent, 1)

	env := &matcherEnv{
		ctx:      context.Background(),
		backend:  backend,
		params:   &params,
		matcher:  newMultiEventMatcher(matchers),
		firstMap: 0,
		lastMap:  0,
		resultCh: resultCh,
	}

	if err := env.runMatcher(matchers, []uint32{mapIndex}); err != nil {
		t.Fatalf("runMatcher returned error: %v", err)
	}

	select {
	case res := <-resultCh:
		if res.BlockNumber != 1 {
			t.Fatalf("unexpected block number: got %d want 1", res.BlockNumber)
		}
	default:
		t.Fatalf("expected a match result, got none")
	}
}

// Ensures multiple matches across map ranges are emitted correctly even when
// some matchers yield zero results.
func TestRunMatcherMultipleMapsMultipleMatches(t *testing.T) {
	params := Params{
		logMapHeight:       3,
		logMapWidth:        4,
		logMapsPerEpoch:    1,
		logValuesPerMap:    4,
		baseRowGroupLength: 4,
		baseRowLengthRatio: 2,
		logLayerDiff:       2,
	}
	params.deriveFields()

	valueA := common.Hash{0xaa}
	valueB := common.Hash{0xbb}

	mapIndexA := uint32(0)
	mapIndexB := uint32(1)

	colA := params.columnIndex(0, &valueA)  // lvIndex 0
	colB := params.columnIndex(18, &valueB) // lvIndex in map 1

	backend := &stubMatcherBackend{
		params: &params,
		rows: map[uint32]FilterRow{
			mapIndexA: {colA},
			mapIndexB: {colB},
		},
		lvRange: lvIndexRange{
			blockNumber:  5,
			startLvIndex: 0,
			lvPointers:   []uint64{0, 5, 10, 20, 40},
		},
	}

	matchers := []*singleMatcher{
		{backend: backend, value: common.Hash{0xcc}}, // zero matches
		{backend: backend, value: valueA},
		{backend: backend, value: valueB},
	}

	resultCh := make(chan *TxEvent, 2)

	env := &matcherEnv{
		ctx:      context.Background(),
		backend:  backend,
		params:   &params,
		matcher:  newMultiEventMatcher(matchers),
		firstMap: mapIndexA,
		lastMap:  mapIndexB,
		resultCh: resultCh,
	}

	if err := env.runMatcher(matchers, []uint32{mapIndexA, mapIndexB}); err != nil {
		t.Fatalf("runMatcher returned error: %v", err)
	}

	var results []*TxEvent
	for i := 0; i < 2; i++ {
		select {
		case res := <-resultCh:
			results = append(results, res)
		default:
			t.Fatalf("expected result %d, got none", i)
		}
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].BlockNumber != 6 || results[1].BlockNumber != 6 {
		t.Fatalf("unexpected block numbers: %+v", results)
	}
	if results[0].TxIndex == results[1].TxIndex {
		t.Fatalf("expected distinct tx indices, got %d and %d", results[0].TxIndex, results[1].TxIndex)
	}
}
