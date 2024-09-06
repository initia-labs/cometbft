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

	"google.golang.org/protobuf/proto"

	v1beta1 "cosmossdk.io/api/cosmos/base/query/v1beta1"
	rstypes "github.com/cometbft/cometbft/rollupsync/types"
	ophostv1 "github.com/initia-labs/OPinit/api/opinit/ophost/v1"
)

var _ rstypes.BatchProvider = (*L1Provider)(nil)

type L1Provider struct {
	logger log.Logger
	cfg    *config.RollupSyncConfig
	client *rpchttp.HTTP

	submitter string
}

func NewL1Provider(logger log.Logger, cfg *config.RollupSyncConfig) (*L1Provider, error) {
	idx := slices.IndexFunc(cfg.RPCServers, func(elem config.RollupSyncRPCConfig) bool {
		return elem.Chain == rstypes.CHAIN_NAME_L1
	})
	if idx < 0 {
		return nil, fmt.Errorf("%s rpc address is not provided", rstypes.CHAIN_NAME_L1)
	}
	client, err := newRpcClient(cfg.RPCServers[idx].Address)
	if err != nil {
		return nil, fmt.Errorf("unable to create RPC client: %w", err)
	}

	return &L1Provider{
		logger: logger,
		cfg:    cfg,
		client: client,
	}, nil
}

func (lp *L1Provider) SetSubmitter(submitter string) {
	lp.submitter = submitter
}

func (lp L1Provider) BatchFetcher(ctx context.Context, batchCh chan<- rstypes.BatchChanInfo, batchChainStartHeight int64, l2EndHeight *uint64) error {
	if lp.submitter == "" {
		return errors.New("submitter is not provided")
	}

	timer := time.NewTicker(time.Duration(lp.cfg.FetchInterval) * time.Millisecond)
	defer timer.Stop()

	page := 1
	height := batchChainStartHeight
	nextHeight := height + lp.cfg.BatchChainQueryHeightRange

	for {
		select {
		case <-ctx.Done():
			lp.logger.Info("Closing batch fetcher")
			return nil
		case <-timer.C:
			if isEnd, lastBatchHeaderStart, err := lp.fetchBatch(ctx, batchCh, page, height, nextHeight); err != nil {
				lp.logger.Debug("Failed fetching batch", "height", height, "page", page, "error", err)
				continue
			} else if !isEnd {
				page++
				continue
			} else if lastBatchHeaderStart != 0 && *l2EndHeight != 0 && lastBatchHeaderStart > *l2EndHeight {
				lp.logger.Debug("reach the end height of this batch info", "batch_header_start", lastBatchHeaderStart, "l2_end_height", *l2EndHeight)
				return nil
			}

			height = nextHeight
			nextHeight = height + lp.cfg.BatchChainQueryHeightRange
			page = 1
		}
	}
}

func (lp L1Provider) FirstTxHeight(ctx context.Context) (int64, error) {
	page := 1
	txsPerPage := 1
	queryStr := fmt.Sprintf("record_batch.submitter='%s'", lp.submitter)
	res, err := lp.client.TxSearch(ctx, queryStr, false, &page, &txsPerPage, "asc")
	if err != nil {
		return 0, err
	} else if len(res.Txs) == 0 {
		return 0, errors.New("no batch txs found")
	}
	return res.Txs[0].Height, nil
}

func (lp L1Provider) fetchBatch(ctx context.Context, batchCh chan<- rstypes.BatchChanInfo, page int, height int64, nextHeight int64) (bool, uint64, error) {
	txsPerPage := int(lp.cfg.TxsPerPage)
	queryStr := fmt.Sprintf("tx.height >= %d AND tx.height < %d AND record_batch.submitter='%s'", height, nextHeight, lp.submitter)
	res, err := lp.client.TxSearch(ctx, queryStr, false, &page, &txsPerPage, "asc")
	if err != nil {
		return false, 0, err
	}

	lastBatchHeaderStart := uint64(0)
	for _, tx := range res.Txs {
		_, body, err := UnmarshalCosmosTx(tx.Tx)
		messages := body.Messages
		if err != nil {
			return false, 0, err
		}

		for _, anyMsg := range messages {
			if anyMsg.TypeUrl != "/opinit.ophost.v1.MsgRecordBatch" {
				continue
			}

			msg := new(ophostv1.MsgRecordBatch)
			err := anyMsg.UnmarshalTo(msg)
			if err != nil {
				return false, 0, err
			}

			_, start, _, err := rstypes.UnmarshalPartialHeader(msg.BatchBytes)
			if err != nil {
				return false, 0, err
			}
			lastBatchHeaderStart = start
			batchCh <- rstypes.BatchChanInfo{
				Batch:            msg.BatchBytes,
				BatchChainHeight: tx.Height,
			}
		}
	}
	return res.TotalCount <= page*txsPerPage, lastBatchHeaderStart, nil
}

func (lp L1Provider) GetLastFinalizedBlock(ctx context.Context) (uint64, error) {
	reqMsg := ophostv1.QueryLastFinalizedOutputRequest{BridgeId: lp.cfg.BridgeID}
	reqBytes, err := proto.Marshal(&reqMsg)
	if err != nil {
		return 0, err
	}

	res, err := lp.client.ABCIQuery(ctx, "/opinit.ophost.v1.Query/LastFinalizedOutput", reqBytes)
	if err != nil {
		return 0, err
	} else if res.Response.Code != 0 {
		return 0, errors.New(res.Response.Log)
	}

	msg := new(ophostv1.QueryLastFinalizedOutputResponse)
	err = proto.Unmarshal(res.Response.Value, msg)
	if err != nil {
		return 0, fmt.Errorf("failed to unmarshal query output response: %v", err)
	}

	return msg.OutputProposal.L2BlockNumber, nil
}

func (lp L1Provider) GetLastHeight(ctx context.Context) (int64, error) {
	resBlock, err := lp.client.Block(ctx, nil)
	if err != nil {
		return 0, err
	}

	return resBlock.Block.Height, nil
}

func (lp L1Provider) GetNextBatchInfo(ctx context.Context, batchInfoIndex uint64) (*ophostv1.BatchInfoWithOutput, error) {
	return lp.GetBatchInfo(ctx, batchInfoIndex+1)
}

func (lp L1Provider) GetBatchInfo(ctx context.Context, batchInfoIndex uint64) (*ophostv1.BatchInfoWithOutput, error) {
	reqMsg := ophostv1.QueryBatchInfosRequest{
		BridgeId: lp.cfg.BridgeID,
		Pagination: &v1beta1.PageRequest{
			Offset: batchInfoIndex,
			Limit:  1,
		},
	}
	reqBytes, err := proto.Marshal(&reqMsg)
	if err != nil {
		return nil, err
	}

	res, err := lp.client.ABCIQuery(ctx, "/opinit.ophost.v1.Query/BatchInfos", reqBytes)
	if err != nil {
		return nil, err
	} else if res.Response.Code != 0 {
		return nil, errors.New(res.Response.Log)
	}

	msg := new(ophostv1.QueryBatchInfosResponse)
	err = proto.Unmarshal(res.Response.Value, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query batch infos response: %v", err)
	}

	if len(msg.BatchInfos) == 0 {
		return nil, nil
	}
	return msg.BatchInfos[0], nil
}

func (lp L1Provider) GetBatchInfos(ctx context.Context) ([]*ophostv1.BatchInfoWithOutput, error) {
	batchInfos := make([]*ophostv1.BatchInfoWithOutput, 0)
	batchInfoIndex := uint64(0)
	for {
		reqMsg := ophostv1.QueryBatchInfosRequest{
			BridgeId: lp.cfg.BridgeID,
			Pagination: &v1beta1.PageRequest{
				Offset: batchInfoIndex,
				Limit:  100,
			},
		}
		reqBytes, err := proto.Marshal(&reqMsg)
		if err != nil {
			return nil, err
		}

		res, err := lp.client.ABCIQuery(ctx, "/opinit.ophost.v1.Query/BatchInfos", reqBytes)
		if err != nil {
			return nil, err
		} else if res.Response.Code != 0 {
			return nil, errors.New(res.Response.Log)
		}

		msg := new(ophostv1.QueryBatchInfosResponse)
		err = proto.Unmarshal(res.Response.Value, msg)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal query batch infos response: %v", err)
		}

		batchInfos = append(batchInfos, msg.BatchInfos...)
		batchInfoIndex += uint64(len(msg.BatchInfos))

		if msg.Pagination.Total == batchInfoIndex {
			break
		}
	}
	return batchInfos, nil
}

func (lp L1Provider) GetOracleTx(ctx context.Context, height int64) ([]byte, error) {
	resBlock, err := lp.client.Block(ctx, &height)
	if err != nil {
		return nil, err
	}
	return resBlock.Block.Txs[0], nil
}
