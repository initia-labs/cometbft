package commands

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/cometbft/cometbft/rollupsync"
	"github.com/spf13/cobra"
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

	genDoc, err := rollupsync.FetchGenesis(ctx, logger, config.RollupSync)
	if err != nil {
		return err
	}

	logger.Info("genesis fetched", "chain-id", genDoc.ChainID, "genesis-time", genDoc.GenesisTime, "initial-height", genDoc.InitialHeight)
	return genDoc.SaveAs(config.GenesisFile())
}
