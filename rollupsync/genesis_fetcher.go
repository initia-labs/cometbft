package rollupsync

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/rollupsync/provider"
	rstypes "github.com/cometbft/cometbft/rollupsync/types"
	"github.com/cometbft/cometbft/types"
)

func FetchGenesis(ctx context.Context, logger log.Logger, cfg *config.RollupSyncConfig) (*types.GenesisDoc, error) {
	l1Provider, err := provider.NewL1Provider(logger, cfg)
	if err != nil {
		return nil, err
	}

	batchInfos, err := l1Provider.GetBatchInfos(ctx)
	if err != nil {
		return nil, err
	}

	if len(batchInfos) == 0 {
		return nil, errors.New("no batch info found")
	}

	batchInfoIndex := 0
	for ; batchInfoIndex < len(batchInfos); batchInfoIndex++ {
		if batchInfos[batchInfoIndex].Output.L2BlockNumber > uint64(1) {
			break
		}
	}
	// There is always first batch info with Output.L2BlockNumber == 0,
	// so batchInfoIndex is always greater than 0
	batchInfoIndex--
	batchInfo := batchInfos[batchInfoIndex]

	batchProvider, err := provider.NewBatchProvider(logger.With("provider", rstypes.BatchChainTypeToString(batchInfo.BatchInfo.ChainType)), cfg, batchInfo.BatchInfo.ChainType, batchInfo.BatchInfo.Submitter, int64(batchInfoIndex))
	if err != nil {
		return nil, err
	}

	batchCh := make(chan rstypes.BatchChanInfo)
	batchChClosed := make(chan struct{})
	defer close(batchChClosed)

	go batchProvider.BatchFetcher(ctx, batchCh, batchChClosed, 1)

	genesisChunks := make(map[int]rstypes.BatchDataGenesis)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case batch := <-batchCh:
			if rstypes.BatchDataType(batch.Batch[0]) == rstypes.BatchDataTypeGenesis {
				genesisChunk, err := rstypes.UnmarshalBatchDataGenesis(batch.Batch)
				if err != nil {
					return nil, err
				}
				genesisChunks[int(genesisChunk.Index)] = genesisChunk
				logger.Info("received a genesis chunk", "index", genesisChunk.Index, "length", genesisChunk.Length)

				if len(genesisChunks) == int(genesisChunk.Length) {
					genesisBz := make([]byte, 0)
					for i := 0; i < int(genesisChunk.Length); i++ {
						genesisBz = append(genesisBz, genesisChunks[i].ChunkData...)
					}

					var genesisDoc types.GenesisDoc
					err = json.Unmarshal(genesisBz, &genesisDoc)
					if err != nil {
						return nil, err
					}

					return &genesisDoc, nil
				}
			}
		}
	}
}
