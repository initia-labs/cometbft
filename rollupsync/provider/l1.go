package provider

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cometbft/cometbft/libs/log"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	v1beta1 "cosmossdk.io/api/cosmos/tx/v1beta1"
	"google.golang.org/protobuf/proto"

	rstypes "github.com/cometbft/cometbft/rollupsync/types"
	ophostv1 "github.com/initia-labs/OPinit/api/opinit/ophost/v1"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

var (
	DEFAULT_FETCH_INTERVAL = 10 // millisecond
	DEFAULT_TXS_PER_PAGE   = 100
)

var _ rstypes.BatchProvider = (*L1Provider)(nil)
var _ rstypes.OutputProvider = (*L1Provider)(nil)

type L1Provider struct {
	logger log.Logger
	client *rpchttp.HTTP

	submitter string

	batchCh chan []byte
}

func NewL1Provider(logger log.Logger, rpcAddress string, submitter string) (*L1Provider, error) {
	client, err := RPCClient(rpcAddress)
	if err != nil {
		return nil, fmt.Errorf("unable to create RPC client: %w", err)
	}

	return &L1Provider{
		logger:    logger,
		client:    client,
		submitter: submitter,
		batchCh:   make(chan []byte, 100),
	}, nil
}

func (lp *L1Provider) BatchFetcher(ctx context.Context) error {
	timer := time.NewTicker(time.Duration(DEFAULT_FETCH_INTERVAL) * time.Millisecond)
	page := 1

LOOP:
	for {
		select {
		case <-ctx.Done():
			lp.logger.Error("Batch fetcher is terminated", ctx.Err())
			return ctx.Err()
		case <-timer.C:
			if isEnd, err := lp.fetchBatch(ctx, page); err != nil {
				lp.logger.Error("Failed fetching batch", "page", page)
				continue LOOP
			} else if isEnd {
				// close(lp.batchCh)
				return nil
			}
			page++
		}
	}
}

func (lp *L1Provider) fetchBatch(ctx context.Context, page int) (bool, error) {
	res, err := lp.client.TxSearch(ctx, "message.action='/opinit.ophost.v1.MsgRecordBatch'", false, &page, &DEFAULT_TXS_PER_PAGE, "asc")
	if err != nil {
		return false, err
	}
	lp.logger.Debug("Fetch batch", "page", page, "txs", len(res.Txs), "total count", res.TotalCount)

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

			lp.batchCh <- msg.BatchBytes
		}
	}

	if res.TotalCount <= page*DEFAULT_TXS_PER_PAGE {
		return true, nil
	}
	return false, nil
}

func unmarshalCosmosTx(txbytes []byte) ([]*anypb.Any, error) {
	var raw v1beta1.TxRaw
	if err := proto.Unmarshal(txbytes, &raw); err != nil {
		return nil, err
	}

	var body v1beta1.TxBody
	if err := proto.Unmarshal(raw.BodyBytes, &body); err != nil {
		return nil, err
	}
	return body.Messages, nil
}

func (lp *L1Provider) GetBatchChannel() <-chan []byte {
	return lp.batchCh
}

func (lp *L1Provider) GetLatestFinalizedBlock(ctx context.Context) (uint64, error) {
	page := 1
	tx_per_page := 1
	res, err := lp.client.TxSearch(ctx, "message.action='/opinit.ophost.v1.MsgProposeOutput'", false, &page, &tx_per_page, "desc")
	if err != nil {
		return 0, err
	}

	for _, tx := range res.Txs {
		messages, err := unmarshalCosmosTx(tx.Tx)
		if err != nil {
			return 0, err
		}

		for _, anyMsg := range messages {
			msg := &ophostv1.MsgProposeOutput{}
			err := anyMsg.UnmarshalTo(msg)
			if err != nil {
				return 0, err
			}
			return msg.L2BlockNumber, nil
		}
	}
	return 0, errors.New("cannot find any proposed output from L1")
}
