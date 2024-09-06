package provider

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	rstypes "github.com/cometbft/cometbft/rollupsync/types"

	coretypes "github.com/cometbft/cometbft/rpc/core/types"
)

var _ rstypes.BatchProvider = (*CelestiaProvider)(nil)

type CelestiaProvider struct {
	logger log.Logger
	cfg    *config.RollupSyncConfig
	client *rpchttp.HTTP

	cachedBlock *coretypes.ResultBlock
	submitter   string
}

func NewCelestiaProvider(logger log.Logger, cfg *config.RollupSyncConfig) (*CelestiaProvider, error) {
	idx := slices.IndexFunc(cfg.RPCServers, func(elem config.RollupSyncRPCConfig) bool {
		return elem.Chain == rstypes.CHAIN_NAME_CELESTIA
	})
	if idx < 0 {
		return nil, fmt.Errorf("%s rpc address is not provided", rstypes.CHAIN_NAME_CELESTIA)
	}
	client, err := newRpcClient(cfg.RPCServers[idx].Address)
	if err != nil {
		return nil, fmt.Errorf("unable to create RPC client: %w", err)
	}

	return &CelestiaProvider{
		logger: logger,
		cfg:    cfg,
		client: client,
	}, nil
}

func (cp *CelestiaProvider) SetSubmitter(submitter string) {
	cp.submitter = submitter
}

func (cp *CelestiaProvider) BatchFetcher(ctx context.Context, batchCh chan<- rstypes.BatchChanInfo, batchChainStartHeight int64, l2EndHeight *uint64) error {
	if cp.submitter == "" {
		return errors.New("submitter is not provided")
	}

	timer := time.NewTicker(time.Duration(cp.cfg.FetchInterval) * time.Millisecond)
	defer timer.Stop()

	page := 1
	height := batchChainStartHeight
	nextHeight := height + cp.cfg.BatchChainQueryHeightRange

	for {
		select {
		case <-ctx.Done():
			cp.logger.Info("Closing batch fetcher")
			return nil
		case <-timer.C:
			if isEnd, lastBatchHeaderStart, err := cp.fetchBatch(ctx, batchCh, page, height, nextHeight); err != nil {
				cp.logger.Debug("Failed fetching batch", "height", height, "page", page, "error", err)
				continue
			} else if !isEnd {
				page++
				continue
			} else if lastBatchHeaderStart != 0 && *l2EndHeight != 0 && lastBatchHeaderStart > *l2EndHeight {
				cp.logger.Debug("reach the end height of this batch info", "batch_header_start", lastBatchHeaderStart, "l2_end_height", *l2EndHeight)
				return nil
			}

			height = nextHeight
			nextHeight = height + cp.cfg.BatchChainQueryHeightRange
			page = 1
		}
	}
}

func (cp *CelestiaProvider) FirstTxHeight(ctx context.Context) (int64, error) {
	page := 1
	txsPerPage := 1
	queryStr := fmt.Sprintf("celestia.blob.v1.EventPayForBlobs.signer='\"%s\"'", cp.submitter)
	res, err := cp.client.TxSearch(ctx, queryStr, false, &page, &txsPerPage, "asc")
	if err != nil {
		return 0, err
	} else if len(res.Txs) == 0 {
		return 0, errors.New("no batch txs found")
	}
	return res.Txs[0].Height, nil
}

func (cp *CelestiaProvider) fetchBatch(ctx context.Context, batchCh chan<- rstypes.BatchChanInfo, page int, height int64, nextHeight int64) (bool, uint64, error) {
	txsPerPage := int(cp.cfg.TxsPerPage)
	queryStr := fmt.Sprintf("tx.height >= %d AND tx.height < %d AND celestia.blob.v1.EventPayForBlobs.signer='\"%s\"'", height, nextHeight, cp.submitter)
	res, err := cp.client.TxSearch(ctx, queryStr, false, &page, &txsPerPage, "asc")
	if err != nil {
		return false, 0, err
	}

	lastBatchHeaderStart := uint64(0)
	for _, tx := range res.Txs {
		if cp.cachedBlock == nil || cp.cachedBlock.Block.Height != tx.Height {
			res, err := cp.client.Block(ctx, &tx.Height)
			if err != nil {
				return false, 0, err
			}
			cp.cachedBlock = res
		}

		txBytes := cp.cachedBlock.Block.Txs[tx.Index]
		blobTx, err := unmarshalCelestiaBlobTx(txBytes)
		if err != nil {
			return false, 0, err
		}

		for _, blob := range blobTx.Blobs {
			_, start, _, err := rstypes.UnmarshalPartialHeader(blob.Data())
			if err != nil {
				return false, 0, err
			}
			lastBatchHeaderStart = start
			batchCh <- rstypes.BatchChanInfo{
				Batch:            blob.Data(),
				BatchChainHeight: tx.Height,
			}
		}
	}
	return res.TotalCount <= page*txsPerPage, lastBatchHeaderStart, nil
}

func (cp CelestiaProvider) GetLastHeight(ctx context.Context) (int64, error) {
	resBlock, err := cp.client.Block(ctx, nil)
	if err != nil {
		return 0, err
	}

	return resBlock.Block.Height, nil
}
