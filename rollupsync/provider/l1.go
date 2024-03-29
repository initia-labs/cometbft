package provider

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	"google.golang.org/protobuf/proto"

	rstypes "github.com/cometbft/cometbft/rollupsync/types"
	ophostv1 "github.com/initia-labs/OPinit/api/opinit/ophost/v1"
)

var _ rstypes.BatchProvider = (*L1Provider)(nil)
var _ rstypes.OutputProvider = (*L1Provider)(nil)

type L1Provider struct {
	logger log.Logger
	client *rpchttp.HTTP

	bridgeId  int64
	submitter string

	batchCh chan rstypes.BatchInfo
	quit    chan struct{}
}

func NewL1Provider(logger log.Logger, bridgeId int64, rpcAddress string, submitter string) (*L1Provider, error) {
	client, err := RPCClient(rpcAddress)
	if err != nil {
		return nil, fmt.Errorf("unable to create RPC client: %w", err)
	}

	return &L1Provider{
		logger:    logger,
		client:    client,
		bridgeId:  bridgeId,
		submitter: submitter,
		batchCh:   make(chan rstypes.BatchInfo, 100),
		quit:      make(chan struct{}, 1),
	}, nil
}

func (lp L1Provider) BatchFetcher(ctx context.Context, startHeight int64, endHeight int64) error {
	timer := time.NewTicker(time.Duration(DEFAULT_FETCH_INTERVAL) * time.Millisecond)
	page := 1
	height := startHeight
	nextHeight := height + int64(DEFAULT_HEIGHT_INTERVAL)

LOOP:
	for {
		select {
		case <-ctx.Done():
			lp.logger.Info("Closing batch fetcher")
			return ctx.Err()
		case <-lp.quit:
			return nil
		case <-timer.C:
			if isEnd, err := lp.fetchBatch(ctx, page, height, nextHeight); err != nil {
				lp.logger.Debug("Failed fetching batch", "height", height, "page", page, "error", err)
				continue LOOP
			} else if isEnd {
				height = nextHeight
				nextHeight = height + int64(DEFAULT_HEIGHT_INTERVAL)
				if height > endHeight {
					return nil
				}
				page = 1
				lp.batchCh <- rstypes.BatchInfo{
					L1QueryHeight: height - 1,
				}
			} else {
				page++
			}
		}
	}
}

func (lp L1Provider) fetchBatch(ctx context.Context, page int, height int64, nextHeight int64) (bool, error) {
	queryStr := fmt.Sprintf("tx.height >= %d AND tx.height < %d AND message.action='/opinit.ophost.v1.MsgRecordBatch' AND message.sender='%s'", height, nextHeight, lp.submitter)
	res, err := lp.client.TxSearch(ctx, queryStr, false, &page, &DEFAULT_TXS_PER_PAGE, "asc")
	if err != nil {
		return false, err
	}
	lp.logger.Debug("Fetch batch", "current", height, "next", nextHeight, "page", page, "txs", len(res.Txs))

	for _, tx := range res.Txs {
		messages, err := unmarshalCosmosTx(tx.Tx)
		if err != nil {
			return false, err
		}
		for _, anyMsg := range messages {
			if anyMsg.TypeUrl != "/opinit.ophost.v1.MsgRecordBatch" {
				continue
			}
			msg := &ophostv1.MsgRecordBatch{}

			err := anyMsg.UnmarshalTo(msg)
			if err != nil {
				return false, err
			}

			lp.batchCh <- rstypes.BatchInfo{
				Batch: msg.BatchBytes,
			}
		}
	}

	if res.TotalCount <= page*DEFAULT_TXS_PER_PAGE {
		return true, nil
	}
	return false, nil
}

func (lp L1Provider) Quit() {
	lp.quit <- struct{}{}
}

func (lp L1Provider) GetBatchChannel() <-chan rstypes.BatchInfo {
	return lp.batchCh
}

func (lp L1Provider) GetLatestFinalizedBlock(ctx context.Context) (uint64, error) {
	var reqMsg ophostv1.QueryLastFinalizedOutputRequest
	reqMsg.BridgeId = uint64(lp.bridgeId)
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

	var msg ophostv1.QueryLastFinalizedOutputResponse

	err = proto.Unmarshal(res.Response.Value, &msg)
	if err != nil {
		return 0, fmt.Errorf("error unmarshalling query output response: %v", err)
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
