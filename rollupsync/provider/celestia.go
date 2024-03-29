package provider

import (
	"context"
	"fmt"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	rstypes "github.com/cometbft/cometbft/rollupsync/types"
)

var _ rstypes.BatchProvider = (*L1Provider)(nil)

type CelestiaProvider struct {
	logger log.Logger
	client *rpchttp.HTTP

	submitter string

	batchCh chan rstypes.BatchInfo
	quit    chan struct{}
}

func NewCelestiaProvider(logger log.Logger, rpcAddress string, submitter string) (*CelestiaProvider, error) {
	client, err := RPCClient(rpcAddress)
	if err != nil {
		return nil, fmt.Errorf("unable to create RPC client: %w", err)
	}

	return &CelestiaProvider{
		logger:    logger,
		client:    client,
		submitter: submitter,
		batchCh:   make(chan rstypes.BatchInfo, 100),
		quit:      make(chan struct{}, 1),
	}, nil
}

func (cp *CelestiaProvider) BatchFetcher(ctx context.Context, startHeight int64, endHeight int64) error {
	timer := time.NewTicker(time.Duration(DEFAULT_FETCH_INTERVAL) * time.Millisecond)
	page := 1
	height := startHeight
	nextHeight := height + int64(DEFAULT_HEIGHT_INTERVAL)
	searchMap := make(map[int64]map[uint32]struct{})

LOOP:
	for {
		select {
		case <-ctx.Done():
			cp.logger.Info("Closing batch fetcher")
			return ctx.Err()
		case <-cp.quit:
			return nil
		case <-timer.C:
			if isEnd, err := cp.fetchBatch(ctx, page, height, nextHeight, searchMap); err != nil {
				cp.logger.Debug("Failed fetching batch", "height", height, "page", page, "error", err)
				continue LOOP
			} else if isEnd {
				height = nextHeight
				nextHeight = height + int64(DEFAULT_HEIGHT_INTERVAL)
				err := cp.fetchBlock(ctx, searchMap)
				if err != nil {
					continue LOOP
				}
				if height > endHeight {
					return nil
				}
				page = 1
				cp.batchCh <- rstypes.BatchInfo{
					L1QueryHeight: height - 1,
				}
			} else {
				page++
			}
		}
	}
}

func (cp *CelestiaProvider) fetchBatch(ctx context.Context, page int, height int64, nextHeight int64, searchMap map[int64]map[uint32]struct{}) (bool, error) {
	queryStr := fmt.Sprintf("tx.height >= %d AND tx.height < %d AND message.action='/celestia.blob.v1.MsgPayForBlobs' AND message.sender='%s'", height, nextHeight, cp.submitter)
	res, err := cp.client.TxSearch(ctx, queryStr, false, &page, &DEFAULT_TXS_PER_PAGE, "asc")
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

	if res.TotalCount <= page*DEFAULT_TXS_PER_PAGE {
		return true, nil
	}
	return false, nil
}

func (cp *CelestiaProvider) fetchBlock(ctx context.Context, searchMap map[int64]map[uint32]struct{}) error {
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
				cp.batchCh <- rstypes.BatchInfo{
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

func (cp CelestiaProvider) Quit() {
	cp.quit <- struct{}{}
}

func (cp CelestiaProvider) GetBatchChannel() <-chan rstypes.BatchInfo {
	return cp.batchCh
}

func (cp CelestiaProvider) GetLastHeight(ctx context.Context) (int64, error) {
	resBlock, err := cp.client.Block(ctx, nil)
	if err != nil {
		return 0, err
	}

	return resBlock.Block.Height, nil
}
