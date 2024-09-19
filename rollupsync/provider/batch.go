package provider

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	ophostv1 "github.com/initia-labs/OPinit/api/opinit/ophost/v1"

	rstypes "github.com/cometbft/cometbft/rollupsync/types"

	coretypes "github.com/cometbft/cometbft/rpc/core/types"
)

type BatchProvider struct {
	logger    log.Logger
	cfg       *config.RollupSyncConfig
	client    *rpchttp.HTTP
	chainType ophostv1.BatchInfo_ChainType

	cachedBlock    *coretypes.ResultBlock
	submitter      string
	batchInfoIndex int64
}

func NewBatchProvider(logger log.Logger, cfg *config.RollupSyncConfig, chainType ophostv1.BatchInfo_ChainType, submitter string, batchInfoIndex int64) (*BatchProvider, error) {
	chainName := strings.ToLower(rstypes.BatchChainTypeToString(chainType))
	idx := slices.IndexFunc(cfg.RPCServers, func(elem config.RollupSyncRPCConfig) bool {
		return elem.Chain == chainName
	})
	if idx < 0 {
		return nil, fmt.Errorf("%s rpc address is not provided", chainName)
	}
	client, err := newRpcClient(cfg.RPCServers[idx].Address)
	if err != nil {
		return nil, fmt.Errorf("unable to create RPC client: %w", err)
	}

	return &BatchProvider{
		logger:         logger,
		cfg:            cfg,
		client:         client,
		chainType:      chainType,
		submitter:      submitter,
		batchInfoIndex: batchInfoIndex,
	}, nil
}

func (bp *BatchProvider) BatchFetcher(ctx context.Context, batchCh chan<- rstypes.BatchChanInfo, batchChClosed <-chan struct{}, batchChainStartHeight int64) {
	timer := time.NewTicker(time.Duration(bp.cfg.FetchInterval) * time.Millisecond)
	defer timer.Stop()

	page := 1
	height := batchChainStartHeight
	nextHeight := height + bp.cfg.BatchChainQueryHeightRange

	for {
		select {
		case <-ctx.Done():
			bp.logger.Info("Closing batch fetcher")
			return
		case <-timer.C:
			if height == 1 {
				firstHeight, err := bp.FirstTxHeight(ctx)
				if err != nil {
					bp.logger.Info("Failed fetching first height", "error", err)
					continue
				}
				height = firstHeight
				nextHeight = height + bp.cfg.BatchChainQueryHeightRange
			}

			if page == 1 {
				latestHeight, err := bp.GetLatestHeight(ctx)
				if err != nil {
					bp.logger.Info("Failed fetching last height", "error", err)
					continue
				} else if latestHeight < nextHeight {
					nextHeight = latestHeight
				}
			}

			if isEnd, err := bp.fetchBatch(ctx, batchCh, batchChClosed, page, height, nextHeight); err != nil {
				bp.logger.Info("Failed fetching batch", "height", height, "page", page, "error", err)
				continue
			} else if !isEnd {
				page++
				continue
			}

			height = nextHeight
			nextHeight = height + bp.cfg.BatchChainQueryHeightRange
			page = 1
		}
	}
}

func (bp *BatchProvider) FirstTxHeight(ctx context.Context) (int64, error) {
	timer := time.NewTicker(time.Duration(bp.cfg.FetchInterval) * time.Millisecond)
	defer timer.Stop()

	page := 1
	txsPerPage := 1
	queryStr := rstypes.QueryEventTypeWithSubmitterFromChainType(bp.chainType, bp.submitter)

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-timer.C:
			res, err := bp.client.TxSearch(ctx, queryStr, false, &page, &txsPerPage, "asc")
			if err != nil {
				bp.logger.Error("Failed fetching first batch", "error", err)
				continue
			} else if len(res.Txs) == 0 {
				bp.logger.Error("Failed fetching first batch; no batch txs found")
				continue
			}
			return res.Txs[0].Height, nil
		}
	}
}

func (bp *BatchProvider) fetchBatch(ctx context.Context, batchCh chan<- rstypes.BatchChanInfo, batchChClosed <-chan struct{}, page int, height int64, nextHeight int64) (bool, error) {
	if height == nextHeight {
		return true, nil
	}

	txsPerPage := int(bp.cfg.TxsPerPage)
	queryStr := fmt.Sprintf("tx.height >= %d AND tx.height < %d AND %s", height, nextHeight, rstypes.QueryEventTypeWithSubmitterFromChainType(bp.chainType, bp.submitter))
	res, err := bp.client.TxSearch(ctx, queryStr, false, &page, &txsPerPage, "asc")
	if err != nil {
		return false, err
	}

	bp.logger.Info("batch chain query range", "chain", rstypes.BatchChainTypeToString(bp.chainType), "range", fmt.Sprintf("%d ~ %d", height, nextHeight), "txs", len(res.Txs))

	for _, tx := range res.Txs {
		batches, err := bp.batchesFromTx(ctx, tx)
		if err != nil {
			return false, err
		}

		for _, batch := range batches {
			select {
			case <-batchChClosed:
			case batchCh <- rstypes.BatchChanInfo{
				BatchInfoIndex:   bp.batchInfoIndex,
				Batch:            batch,
				BatchChainHeight: tx.Height,
			}:
			}

		}
	}
	return res.TotalCount <= page*txsPerPage, nil
}

func (bp *BatchProvider) batchesFromTx(ctx context.Context, tx *coretypes.ResultTx) ([][]byte, error) {
	switch bp.chainType {
	case ophostv1.BatchInfo_CHAIN_TYPE_INITIA:
		return bp.batchesFromL1Tx(tx)
	case ophostv1.BatchInfo_CHAIN_TYPE_CELESTIA:
		return bp.batchesFromCelestiaTx(ctx, tx)
	default:
		return nil, errors.New("unsupported chain type")
	}
}

func (bp *BatchProvider) batchesFromL1Tx(tx *coretypes.ResultTx) ([][]byte, error) {
	_, body, err := UnmarshalCosmosTx(tx.Tx)
	if err != nil {
		return nil, err
	}

	for _, anyMsg := range body.Messages {
		if anyMsg.TypeUrl != "/opinit.ophost.v1.MsgRecordBatch" {
			continue
		}

		msg := new(ophostv1.MsgRecordBatch)
		err := anyMsg.UnmarshalTo(msg)
		if err != nil {
			return nil, err
		}

		return [][]byte{msg.BatchBytes}, nil
	}

	return nil, errors.New("no batch data found in the tx")
}

func (bp *BatchProvider) batchesFromCelestiaTx(ctx context.Context, tx *coretypes.ResultTx) ([][]byte, error) {
	_, body, err := UnmarshalCosmosTx(tx.Tx)
	if err != nil {
		return nil, err
	}

	for _, anyMsg := range body.Messages {
		if anyMsg.TypeUrl != "/celestia.blob.v1.MsgPayForBlobs" {
			return nil, nil
		}
	}

	if bp.cachedBlock == nil || bp.cachedBlock.Block.Height != tx.Height {
		res, err := bp.client.Block(ctx, &tx.Height)
		if err != nil {
			return nil, err
		}
		bp.cachedBlock = res
	}

	txBytes := bp.cachedBlock.Block.Txs[tx.Index]
	blobTx, err := unmarshalCelestiaBlobTx(txBytes)
	if err != nil {
		return nil, err
	}

	data := make([][]byte, 0, len(blobTx.Blobs))
	for _, blob := range blobTx.Blobs {
		data = append(data, blob.Data())
	}
	return data, nil
}

func (bp BatchProvider) GetLatestHeight(ctx context.Context) (int64, error) {
	resBlock, err := bp.client.Block(ctx, nil)
	if err != nil {
		return 0, err
	}

	return resBlock.Block.Height, nil
}
