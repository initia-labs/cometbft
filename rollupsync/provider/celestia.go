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
)

var _ rstypes.BatchProvider = (*CelestiaProvider)(nil)

type CelestiaProvider struct {
	logger log.Logger
	cfg    *config.RollupSyncConfig
	client *rpchttp.HTTP

	submitter string
}

func NewCelestiaProvider(logger log.Logger, cfg *config.RollupSyncConfig) (*CelestiaProvider, error) {
	idx := slices.IndexFunc(cfg.RPCServers, func(elem config.RollupSyncRPCConfig) bool {
		return elem.Chain == rstypes.CHAIN_NAME_CELESTIA
	})
	if idx < 0 {
		return nil, fmt.Errorf("%s rpc address is not provided", rstypes.CHAIN_NAME_CELESTIA)
	}
	client, err := RPCClient(cfg.RPCServers[idx].Address)
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

func (cp *CelestiaProvider) BatchFetcher(ctx context.Context, batchCh chan<- rstypes.BatchInfo, startHeight int64, endHeight int64) error {
	if cp.submitter == "" {
		return errors.New("submitter is not provided")
	}

	timer := time.NewTicker(time.Duration(cp.cfg.FetchInterval) * time.Millisecond)
	defer timer.Stop()
	page := 1
	height := startHeight
	nextHeight := height + int64(cp.cfg.BatchChainQueryHeightInterval)
	searchMap := make(map[int64]map[uint32]struct{})

LOOP:
	for {
		select {
		case <-ctx.Done():
			cp.logger.Info("Closing batch fetcher")
			return nil
		case <-timer.C:
			if isEnd, err := cp.fetchBatch(ctx, page, height, nextHeight, searchMap); err != nil {
				cp.logger.Debug("Failed fetching batch", "height", height, "page", page, "error", err)
				continue LOOP
			} else if isEnd {
				height = nextHeight
				nextHeight = height + int64(cp.cfg.BatchChainQueryHeightInterval)
				err := cp.fetchBlock(ctx, batchCh, searchMap)
				if err != nil {
					continue LOOP
				}
				if height > endHeight {
					return nil
				}
				page = 1
				batchCh <- rstypes.BatchInfo{
					BatchChainHeight: height - 1,
				}
			} else {
				page++
			}
		}
	}
}

func (cp *CelestiaProvider) fetchBatch(ctx context.Context, page int, height int64, nextHeight int64, searchMap map[int64]map[uint32]struct{}) (bool, error) {
	txsPerPage := int(cp.cfg.TxsPerPage)
	queryStr := fmt.Sprintf("tx.height >= %d AND tx.height < %d AND message.action='/celestia.blob.v1.MsgPayForBlobs' AND message.sender='%s'", height, nextHeight, cp.submitter)
	res, err := cp.client.TxSearch(ctx, queryStr, false, &page, &txsPerPage, "asc")
	if err != nil {
		return false, err
	}
	cp.logger.Debug("Fetch batch", "current", height, "next", nextHeight, "page", page, "txs", len(res.Txs))

	for _, tx := range res.Txs {
		if _, ok := searchMap[tx.Height]; !ok {
			searchMap[tx.Height] = make(map[uint32]struct{})
		}
		searchMap[tx.Height][tx.Index] = struct{}{}
	}

	if res.TotalCount <= page*txsPerPage {
		return true, nil
	}
	return false, nil
}

func (cp *CelestiaProvider) fetchBlock(ctx context.Context, batchCh chan<- rstypes.BatchInfo, searchMap map[int64]map[uint32]struct{}) error {
	for height, indexes := range searchMap {
		res, err := cp.client.Block(ctx, &height)
		if err != nil {
			return err
		}

		for index := range indexes {
			txbytes := res.Block.Txs[index]
			blobTx, err := unmarshalCelestiaBlobTx(txbytes)
			if err != nil {
				return err
			}
			for _, blob := range blobTx.Blobs {
				batchCh <- rstypes.BatchInfo{
					Batch: blob.Data,
				}
			}

			delete(searchMap[height], index)
			if len(searchMap[height]) == 0 {
				delete(searchMap, height)
			}
		}
	}
	return nil
}

func (cp CelestiaProvider) GetLastHeight(ctx context.Context) (int64, error) {
	resBlock, err := cp.client.Block(ctx, nil)
	if err != nil {
		return 0, err
	}

	return resBlock.Block.Height, nil
}
