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

func (cp *CelestiaProvider) BatchFetcher(ctx context.Context, batchCh chan<- rstypes.BatchChanInfo, startHeight int64, endHeight int64) error {
	if cp.submitter == "" {
		return errors.New("submitter is not provided")
	}

	timer := time.NewTicker(time.Duration(cp.cfg.FetchInterval) * time.Millisecond)
	defer timer.Stop()

	page := 1
	height := startHeight
	nextHeight := height + int64(cp.cfg.BatchChainQueryHeightRange)
	txIndexMap := make(map[int64][]uint32)

	for {
		select {
		case <-ctx.Done():
			cp.logger.Info("Closing batch fetcher")
			return nil
		case <-timer.C:
			if isEnd, err := cp.searchBatchTxs(ctx, page, height, nextHeight, txIndexMap); err != nil {
				cp.logger.Debug("Failed search batch txs", "height", height, "nextHeight", nextHeight, "page", page, "error", err)
				continue
			} else if !isEnd {
				page++
				continue
			}

			if err := cp.fetchBatch(ctx, batchCh, txIndexMap); err != nil {
				cp.logger.Debug("Failed fetch batch", "height", height, "next_height", nextHeight, "error", err)
				continue
			}

			batchCh <- rstypes.BatchChanInfo{
				BatchChainHeight: nextHeight - 1,
			}

			height = nextHeight
			nextHeight = height + int64(cp.cfg.BatchChainQueryHeightRange)
			if height > endHeight {
				return nil
			}

			page = 1
		}
	}
}

func (cp *CelestiaProvider) searchBatchTxs(ctx context.Context, page int, height int64, nextHeight int64, txIndexMap map[int64][]uint32) (bool, error) {
	txsPerPage := int(cp.cfg.TxsPerPage)
	queryStr := fmt.Sprintf("tx.height >= %d AND tx.height < %d AND message.action='/celestia.blob.v1.MsgPayForBlobs' AND message.sender='%s'", height, nextHeight, cp.submitter)
	res, err := cp.client.TxSearch(ctx, queryStr, false, &page, &txsPerPage, "asc")
	if err != nil {
		return false, err
	}

	cp.logger.Debug("Fetch batch", "height", height, "next_height", nextHeight, "page", page, "num_txs", len(res.Txs))

	for _, tx := range res.Txs {
		if _, ok := txIndexMap[tx.Height]; !ok {
			txIndexMap[tx.Height] = make([]uint32, 0)
		}

		// only need tx index to fetch blob data from tx bytes in a block
		txIndexMap[tx.Height] = append(txIndexMap[tx.Height], tx.Index)
	}

	return res.TotalCount <= page*txsPerPage, nil
}

func (cp *CelestiaProvider) fetchBatch(ctx context.Context, batchCh chan<- rstypes.BatchChanInfo, txIndexMap map[int64][]uint32) error {
	heights := make([]int64, 0)
	for height := range txIndexMap {
		heights = append(heights, height)
	}
	slices.Sort(heights)

	for _, height := range heights {
		res, err := cp.client.Block(ctx, &height)
		if err != nil {
			return err
		}

		slices.Sort(txIndexMap[height])

		for _, index := range txIndexMap[height] {
			txBytes := res.Block.Txs[index]
			blobTx, err := unmarshalCelestiaBlobTx(txBytes)
			if err != nil {
				return err
			}

			for _, blob := range blobTx.Blobs {
				batchCh <- rstypes.BatchChanInfo{
					Batch: blob.Data,
				}
			}
		}

		delete(txIndexMap, height)
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
