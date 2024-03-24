package provider

import (
	"errors"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/rollupsync/types"
)

func NewProviders(cfg config.RollupSyncConfig, logger log.Logger) (types.BatchProvider, types.OutputProvider, error) {
	L1Provider, err := NewL1Provider(logger.With("provider", "l1"), cfg.L1RPC, cfg.BatchSubmitter)
	if err != nil {
		return nil, nil, err
	}

	switch cfg.BatchChain {
	case "l1":
		return L1Provider, L1Provider, nil
	case "celestia":
		return nil, nil, errors.New("not implemented")
	}

	return nil, nil, errors.New("not implemented")
}
