package commands

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/cometbft/cometbft/rollupsync"
	"github.com/spf13/cobra"
)

var (
	genesisChainID      string
	genesisTimestamp    int64
	initialSearchHeight int64
)

var FetchGenesisCmd = &cobra.Command{
	Use:     "fetch-genesis",
	Aliases: []string{"fetch_genesis"},
	Short:   "Fetch genesis file from DA for rollup sync",
	RunE:    fetchGenesis,
}

func fetchGenesis(cmd *cobra.Command, _ []string) error {
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-c
		cancel()
	}()

	config, err := ParseConfig(cmd)
	if err != nil {
		return err
	}

	genDoc, err := rollupsync.FetchGenesis(ctx, logger, config.RollupSync, genesisChainID, genesisTimestamp, initialSearchHeight)
	if err != nil {
		return err
	}

	logger.Info("genesis fetched", "chain-id", genDoc.ChainID, "genesis-time", genDoc.GenesisTime, "initial-height", genDoc.InitialHeight)
	return genDoc.SaveAs(config.GenesisFile())
}

func init() {
	FetchGenesisCmd.Flags().StringVar(&genesisChainID, "chain-id", "", "the chain id of the genesis file, if not provided, any chain id is accepted")
	FetchGenesisCmd.Flags().Int64Var(&genesisTimestamp, "timestamp", 0, "the minimum genesis timestamp(milliseconds) of the genesis file, if not provided, any timestamp is accepted")
	FetchGenesisCmd.Flags().Int64Var(&initialSearchHeight, "initial-search-height", 0, "the initial search height of the DA chain")
}
