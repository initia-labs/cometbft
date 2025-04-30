package commands

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"

	cmtcfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/internal/test"
	"github.com/cometbft/cometbft/state/mocks"
)

const (
	height int64 = 10
	base   int64 = 2
)

func setupReIndexEventCmd() *cobra.Command {
	reIndexEventCmd := &cobra.Command{
		Use: ReIndexEventCmd.Use,
		Run: func(cmd *cobra.Command, args []string) {},
	}

	_ = reIndexEventCmd.ExecuteContext(context.Background())

	return reIndexEventCmd
}

func TestReIndexEventCheckHeight(t *testing.T) {
	mockBlockStore := &mocks.BlockStore{}
	mockBlockStore.
		On("Base").Return(base).
		On("Height").Return(height)

	testCases := []struct {
		startHeight int64
		endHeight   int64
		validHeight bool
	}{
		{0, 0, true},
		{0, base, true},
		{0, base - 1, false},
		{0, height, true},
		{0, height + 1, true},
		{0, 0, true},
		{base - 1, 0, false},
		{base, 0, true},
		{base, base, true},
		{base, base - 1, false},
		{base, height, true},
		{base, height + 1, true},
		{height, 0, true},
		{height, base, false},
		{height, height - 1, false},
		{height, height, true},
		{height, height + 1, true},
		{height + 1, 0, false},
	}

	for _, tc := range testCases {
		startHeight = tc.startHeight
		endHeight = tc.endHeight

		err := checkValidHeight(mockBlockStore)
		if tc.validHeight {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func TestLoadEventSink(t *testing.T) {
	testCases := []struct {
		sinks   string
		connURL string
		loadErr bool
	}{
		{"", "", true},
		{"NULL", "", true},
		{"KV", "", false},
		// {"PSQL", "", true}, // true because empty connect url
		// skip to test PSQL connect with correct url
		{"UnsupportedSinkType", "wrongUrl", true},
	}

	for idx, tc := range testCases {
		cfg := cmtcfg.TestConfig()
		cfg.TxIndex.Indexer = tc.sinks
		// cfg.TxIndex.PsqlConn = tc.connURL
		_, _, err := loadEventSinks(cfg, test.DefaultTestChainID, nil, nil)
		if tc.loadErr {
			require.Error(t, err, idx)
		} else {
			require.NoError(t, err, idx)
		}
	}
}

func TestLoadBlockStore(t *testing.T) {
	cfg := cmtcfg.TestConfig()
	cfg.DBPath = t.TempDir()
	_, _, err := loadStateAndBlockStore(cfg)
	require.Error(t, err)

	_, err = dbm.NewDB("blockstore", dbm.GoLevelDBBackend, cfg.DBDir())
	require.NoError(t, err)

	// Get StateStore
	_, err = dbm.NewDB("state", dbm.GoLevelDBBackend, cfg.DBDir())
	require.NoError(t, err)

	bs, ss, err := loadStateAndBlockStore(cfg)
	require.NoError(t, err)
	require.NotNil(t, bs)
	require.NotNil(t, ss)
}
