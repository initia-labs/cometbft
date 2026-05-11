package state_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/internal/test"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
)

// setupTestCase does setup common to all test cases.
func setupTestCase(t *testing.T) (func(t *testing.T), dbm.DB, sm.State) {
	t.Helper()
	tearDown, stateDB, state, _ := setupTestCaseWithStore(t)
	return tearDown, stateDB, state
}

// setupTestCase does setup common to all test cases.
func setupTestCaseWithStore(t *testing.T) (func(t *testing.T), dbm.DB, sm.State, sm.Store) {
	t.Helper()
	config := test.ResetTestRoot("state_")
	dbType := dbm.BackendType(config.DBBackend)
	stateDB, err := dbm.NewDB("state", dbType, config.DBDir())
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{
		DiscardABCIResponses: false,
	})
	require.NoError(t, err)
	state, err := stateStore.LoadFromDBOrGenesisFile(config.GenesisFile())
	require.NoError(t, err, "expected no error on LoadStateFromDBOrGenesisFile")
	err = stateStore.Save(state)
	require.NoError(t, err)

	tearDown := func(t *testing.T) {
		t.Helper()
		os.RemoveAll(config.RootDir)
	}

	return tearDown, stateDB, state, stateStore
}

// TestStateCopy tests the correct copying behavior of State.
func TestStateCopy(t *testing.T) {
	tearDown, _, state := setupTestCase(t)
	defer tearDown(t)
	assert := assert.New(t)

	stateCopy := state.Copy()

	assert.True(state.Equals(stateCopy),
		fmt.Sprintf("expected state and its copy to be identical.\ngot: %v\nexpected: %v\n",
			stateCopy, state))

	stateCopy.LastBlockHeight++
	stateCopy.LastValidators = state.Validators
	assert.False(state.Equals(stateCopy), fmt.Sprintf(`expected states to be different. got same
        %v`, state))
}

// TestMakeGenesisStateNilValidators tests state's consistency when genesis file's validators field is nil.
func TestMakeGenesisStateNilValidators(t *testing.T) {
	doc := types.GenesisDoc{
		ChainID:    "dummy",
		Validators: nil,
	}
	require.Nil(t, doc.ValidateAndComplete())
	state, err := sm.MakeGenesisState(&doc)
	require.Nil(t, err)
	require.Equal(t, 0, len(state.Validators.Validators))
	require.Equal(t, 0, len(state.NextValidators.Validators))
}

// TestStateSaveLoad tests saving and loading State from a db.
func TestStateSaveLoad(t *testing.T) {
	tearDown, stateDB, state := setupTestCase(t)
	defer tearDown(t)
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{
		DiscardABCIResponses: false,
	})
	assert := assert.New(t)

	state.LastBlockHeight++
	state.LastValidators = state.Validators
	err := stateStore.Save(state)
	require.NoError(t, err)

	loadedState, err := stateStore.Load()
	require.NoError(t, err)
	assert.True(state.Equals(loadedState),
		fmt.Sprintf("expected state and its copy to be identical.\ngot: %v\nexpected: %v\n",
			loadedState, state))
}

// TestFinalizeBlockResponsesSaveLoad1 tests saving and loading ABCIResponses.
func TestFinalizeBlockResponsesSaveLoad1(t *testing.T) {
	tearDown, stateDB, state := setupTestCase(t)
	defer tearDown(t)
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{
		DiscardABCIResponses: false,
	})
	assert := assert.New(t)

	state.LastBlockHeight++

	// Build mock responses.
	block, err := makeBlock(state, 2, new(types.Commit))
	require.NoError(t, err)

	abciResponses := new(abci.ResponseFinalizeBlock)
	dtxs := make([]*abci.ExecTxResult, 2)
	abciResponses.TxResults = dtxs

	abciResponses.TxResults[0] = &abci.ExecTxResult{Data: []byte("foo"), Events: nil}
	abciResponses.TxResults[1] = &abci.ExecTxResult{Data: []byte("bar"), Log: "ok", Events: nil}
	abciResponses.ValidatorUpdates = []abci.ValidatorUpdate{
		types.TM2PB.NewValidatorUpdate(ed25519.GenPrivKey().PubKey(), 10),
	}

	abciResponses.AppHash = make([]byte, 1)

	err = stateStore.SaveFinalizeBlockResponse(block.Height, abciResponses)
	require.NoError(t, err)
	loadedABCIResponses, err := stateStore.LoadFinalizeBlockResponse(block.Height)
	assert.NoError(err)
	assert.Equal(abciResponses, loadedABCIResponses)
}

// TestResultsSaveLoad tests saving and loading FinalizeBlock results.
func TestFinalizeBlockResponsesSaveLoad2(t *testing.T) {
	tearDown, stateDB, _ := setupTestCase(t)
	defer tearDown(t)
	assert := assert.New(t)

	stateStore := sm.NewStore(stateDB, sm.StoreOptions{
		DiscardABCIResponses: false,
	})

	cases := [...]struct {
		// Height is implied to equal index+2,
		// as block 1 is created from genesis.
		added    []*abci.ExecTxResult
		expected []*abci.ExecTxResult
	}{
		0: {
			nil,
			nil,
		},
		1: {
			[]*abci.ExecTxResult{
				{Code: 32, Data: []byte("Hello"), Log: "Huh?"},
			},
			[]*abci.ExecTxResult{
				{Code: 32, Data: []byte("Hello")},
			},
		},
		2: {
			[]*abci.ExecTxResult{
				{Code: 383},
				{
					Data: []byte("Gotcha!"),
					Events: []abci.Event{
						{Type: "type1", Attributes: []abci.EventAttribute{{Key: "a", Value: "1"}}},
						{Type: "type2", Attributes: []abci.EventAttribute{{Key: "build", Value: "stuff"}}},
					},
				},
			},
			[]*abci.ExecTxResult{
				{Code: 383, Data: nil},
				{Code: 0, Data: []byte("Gotcha!"), Events: []abci.Event{
					{Type: "type1", Attributes: []abci.EventAttribute{{Key: "a", Value: "1"}}},
					{Type: "type2", Attributes: []abci.EventAttribute{{Key: "build", Value: "stuff"}}},
				}},
			},
		},
		3: {
			nil,
			nil,
		},
		4: {
			[]*abci.ExecTxResult{nil},
			nil,
		},
	}

	// Query all before, this should return error.
	for i := range cases {
		h := int64(i + 1)
		res, err := stateStore.LoadFinalizeBlockResponse(h)
		assert.Error(err, "%d: %#v", i, res)
	}

	// Add all cases.
	for i, tc := range cases {
		h := int64(i + 1) // last block height, one below what we save
		responses := &abci.ResponseFinalizeBlock{
			TxResults: tc.added,
			AppHash:   []byte(fmt.Sprintf("%d", h)),
		}
		err := stateStore.SaveFinalizeBlockResponse(h, responses)
		require.NoError(t, err)
	}

	// Query all before, should return expected value.
	for i, tc := range cases {
		h := int64(i + 1)
		res, err := stateStore.LoadFinalizeBlockResponse(h)
		if assert.NoError(err, "%d", i) {
			t.Log(res)
			responses := &abci.ResponseFinalizeBlock{
				TxResults: tc.expected,
				AppHash:   []byte(fmt.Sprintf("%d", h)),
			}
			assert.Equal(sm.TxResultsHash(responses.TxResults), sm.TxResultsHash(res.TxResults), "%d", i)
		}
	}
}

// TestValidatorSimpleSaveLoad tests saving and loading validators.
func TestValidatorSimpleSaveLoad(t *testing.T) {
	tearDown, stateDB, state := setupTestCase(t)
	defer tearDown(t)
	assert := assert.New(t)

	statestore := sm.NewStore(stateDB, sm.StoreOptions{
		DiscardABCIResponses: false,
	})

	// Can't load anything for height 0.
	_, err := statestore.LoadValidators(0)
	assert.IsType(sm.ErrNoValSetForHeight{}, err, "expected err at height 0")

	// Should be able to load for height 1.
	v, err := statestore.LoadValidators(1)
	assert.Nil(err, "expected no err at height 1")
	assert.Equal(v.Hash(), state.Validators.Hash(), "expected validator hashes to match")

	// Should be able to load for height 2.
	v, err = statestore.LoadValidators(2)
	assert.Nil(err, "expected no err at height 2")
	assert.Equal(v.Hash(), state.NextValidators.Hash(), "expected validator hashes to match")

	// Increment height, save; should be able to load for next & next next height.
	state.LastBlockHeight++
	nextHeight := state.LastBlockHeight + 1
	err = statestore.Save(state)
	require.NoError(t, err)
	vp0, err := statestore.LoadValidators(nextHeight + 0)
	assert.Nil(err, "expected no err")
	vp1, err := statestore.LoadValidators(nextHeight + 1)
	assert.Nil(err, "expected no err")
	assert.Equal(vp0.Hash(), state.Validators.Hash(), "expected validator hashes to match")
	assert.Equal(vp1.Hash(), state.NextValidators.Hash(), "expected next validator hashes to match")
}

// TestSequencerValidatorChangesSaveLoad tests saving and loading validator set
// history when the single sequencer validator is replaced.
func TestSequencerValidatorChangesSaveLoad(t *testing.T) {
	tearDown, stateDB, state := setupTestCase(t)
	defer tearDown(t)
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{
		DiscardABCIResponses: false,
	})
	state.Validators = genValSet(1)
	state.NextValidators = state.Validators.CopyIncrementProposerPriority(1)
	err := stateStore.Save(state)
	require.NoError(t, err)
	initialSequencerAddr := getSequencer(state.Validators).Address

	// Change vals at these heights.
	changeHeights := []int64{1, 2, 4, 5, 10, 15, 16, 17, 20}
	N := len(changeHeights)

	// Build the validator history by running updateState
	// with the right validator set for each height.
	highestHeight := changeHeights[N-1] + 5
	changeIndex := 0
	replacementAddrs := make(map[int64][]byte, N)
	var validatorUpdates []*types.Validator
	for i := int64(1); i < highestHeight; i++ {
		pubkey := getSequencer(state.NextValidators).PubKey
		if changeIndex < len(changeHeights) && i == changeHeights[changeIndex] {
			pubkey = ed25519.GenPrivKey().PubKey()
			replacementAddrs[i] = pubkey.Address()
			changeIndex++
		}
		header, blockID, responses := makeHeaderPartsResponsesValPubKeyChange(state, pubkey)
		validatorUpdates, err = types.PB2TM.ValidatorUpdates(responses.ValidatorUpdates)
		require.NoError(t, err)
		state, err = sm.UpdateState(state, blockID, &header, responses, validatorUpdates)
		require.NoError(t, err)
		err = stateStore.Save(state)
		require.NoError(t, err)
	}

	testCases := make([][]byte, highestHeight)
	changeIndex = 0
	expectedAddr := initialSequencerAddr
	for i := int64(1); i < highestHeight+1; i++ {
		// We get to the height after a change height use the next validator (note
		// our counter starts at 0 this time).
		if changeIndex < len(changeHeights) && i == changeHeights[changeIndex]+1 {
			expectedAddr = replacementAddrs[changeHeights[changeIndex]]
			changeIndex++
		}
		testCases[i-1] = expectedAddr
	}

	for i, expectedAddr := range testCases {
		v, err := stateStore.LoadValidators(int64(i + 1 + 1)) // +1 because vset changes delayed by 1 block.
		assert.Nil(t, err, fmt.Sprintf("expected no err at height %d", i))
		assert.Equal(t, v.Size(), 1, "validator set size is not 1: %d", v.Size())
		_, val := v.GetByAddress(expectedAddr)
		require.NotNil(t, val)
		assert.Equal(t, int64(types.SequencerVotingPower), val.VotingPower, "unexpected power at height %d", i)
	}
}

func TestProposerFrequency(t *testing.T) {
	testCases := []struct {
		name   string
		powers []int64
	}{
		{"single sequencer", []int64{types.SequencerVotingPower}},
		{"sequencer and attestor", []int64{types.SequencerVotingPower, types.AttestorVotingPower}},
		{"sequencer and multiple attestors", []int64{types.SequencerVotingPower, types.AttestorVotingPower, 100, 1000}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vals := make([]*types.Validator, len(tc.powers))
			var sequencerAddr []byte
			for i, power := range tc.powers {
				vals[i] = types.NewValidator(ed25519.GenPrivKey().PubKey(), power)
				if power == types.SequencerVotingPower {
					sequencerAddr = vals[i].Address
				}
			}
			valSet := types.NewValidatorSet(vals)
			for i := 0; i < 10; i++ {
				assert.True(t, bytes.Equal(sequencerAddr, valSet.GetProposer().Address))
				valSet.IncrementProposerPriority(1)
			}
		})
	}
}

func TestStoreLoadValidatorsPreservesSingleSequencer(t *testing.T) {
	const valSetSize = 1
	tearDown, stateDB, state := setupTestCase(t)
	t.Cleanup(func() { tearDown(t) })
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{
		DiscardABCIResponses: false,
	})
	state.Validators = genValSet(valSetSize)
	state.NextValidators = state.Validators.CopyIncrementProposerPriority(1)
	err := stateStore.Save(state)
	require.NoError(t, err)

	nextHeight := state.LastBlockHeight + 1

	v0, err := stateStore.LoadValidators(nextHeight)
	assert.Nil(t, err)
	require.Equal(t, 1, v0.Size())
	assert.Equal(t, int64(types.SequencerVotingPower), v0.GetProposer().VotingPower)

	v1, err := stateStore.LoadValidators(nextHeight + 1)
	assert.Nil(t, err)
	require.Equal(t, 1, v1.Size())
	assert.Equal(t, v0.GetProposer().Address, v1.GetProposer().Address)
	assert.Equal(t, int64(types.SequencerVotingPower), v1.GetProposer().VotingPower)
}

func TestStateMakeBlock(t *testing.T) {
	tearDown, _, state := setupTestCase(t)
	defer tearDown(t)

	proposerAddress := state.Validators.GetProposer().Address
	stateVersion := state.Version.Consensus
	block, err := makeBlock(state, 2, new(types.Commit))
	require.NoError(t, err)

	// test we set some fields
	assert.Equal(t, stateVersion, block.Version)
	assert.Equal(t, proposerAddress, block.ProposerAddress)
}

// TestConsensusParamsChangesSaveLoad tests saving and loading consensus params
// with changes.
func TestConsensusParamsChangesSaveLoad(t *testing.T) {
	tearDown, stateDB, state := setupTestCase(t)
	defer tearDown(t)

	stateStore := sm.NewStore(stateDB, sm.StoreOptions{
		DiscardABCIResponses: false,
	})

	// Change vals at these heights.
	changeHeights := []int64{1, 2, 4, 5, 10, 15, 16, 17, 20}
	N := len(changeHeights)

	// Each valset is just one validator.
	// create list of them.
	params := make([]types.ConsensusParams, N+1)
	params[0] = state.ConsensusParams
	for i := 1; i < N+1; i++ {
		params[i] = *types.DefaultConsensusParams()
		params[i].Block.MaxBytes += int64(i)

	}

	// Build the params history by running updateState
	// with the right params set for each height.
	highestHeight := changeHeights[N-1] + 5
	changeIndex := 0
	cp := params[changeIndex]
	var err error
	var validatorUpdates []*types.Validator
	for i := int64(1); i < highestHeight; i++ {
		// When we get to a change height, use the next params.
		if changeIndex < len(changeHeights) && i == changeHeights[changeIndex] {
			changeIndex++
			cp = params[changeIndex]
		}
		header, blockID, responses := makeHeaderPartsResponsesParams(state, cp.ToProto())
		validatorUpdates, err = types.PB2TM.ValidatorUpdates(responses.ValidatorUpdates)
		require.NoError(t, err)
		state, err = sm.UpdateState(state, blockID, &header, responses, validatorUpdates)

		require.NoError(t, err)
		err = stateStore.Save(state)
		require.NoError(t, err)
	}

	// Make all the test cases by using the same params until after the change.
	testCases := make([]paramsChangeTestCase, highestHeight)
	changeIndex = 0
	cp = params[changeIndex]
	for i := int64(1); i < highestHeight+1; i++ {
		// We get to the height after a change height use the next pubkey (note
		// our counter starts at 0 this time).
		if changeIndex < len(changeHeights) && i == changeHeights[changeIndex]+1 {
			changeIndex++
			cp = params[changeIndex]
		}
		testCases[i-1] = paramsChangeTestCase{i, cp}
	}

	for _, testCase := range testCases {
		p, err := stateStore.LoadConsensusParams(testCase.height)
		assert.Nil(t, err, fmt.Sprintf("expected no err at height %d", testCase.height))
		assert.EqualValues(t, testCase.params, p, fmt.Sprintf(`unexpected consensus params at
                height %d`, testCase.height))
	}
}

func TestStateProto(t *testing.T) {
	tearDown, _, state := setupTestCase(t)
	defer tearDown(t)

	tc := []struct {
		testName string
		state    *sm.State
		expPass1 bool
		expPass2 bool
	}{
		{"empty state", &sm.State{}, true, false},
		{"nil failure state", nil, false, false},
		{"success state", &state, true, true},
	}

	for _, tt := range tc {

		pbs, err := tt.state.ToProto()
		if !tt.expPass1 {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err, tt.testName)
		}

		smt, err := sm.FromProto(pbs)
		if tt.expPass2 {
			require.NoError(t, err, tt.testName)
			require.Equal(t, tt.state, smt, tt.testName)
		} else {
			require.Error(t, err, tt.testName)
		}
	}
}
