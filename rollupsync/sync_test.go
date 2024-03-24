package rollupsync

import (
	"context"
	"testing"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/stretchr/testify/require"

	sm "github.com/cometbft/cometbft/state"
)

func TestSync(t *testing.T) {
	cfg := config.DefaultRollupSyncConfig()
	cfg.BatchSubmitter = "init12hkj5y7ryfvu7my6u3g9l3yh9uvatn3yn8zrfu"

	syncer, err := NewRollupSyncer(*cfg, log.TestingLogger().With("module", "rollupsync"), sm.State{}, nil, nil)
	require.NoError(t, err)

	_, err = syncer.Start(context.TODO())
	require.NoError(t, err)

}
