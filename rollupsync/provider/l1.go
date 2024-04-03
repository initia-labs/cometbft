package provider

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	"google.golang.org/protobuf/proto"

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
	client, err := RPCClient(cfg.RPCServers[idx].Address)
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

func (lp L1Provider) BatchFetcher(ctx context.Context, batchCh chan<- rstypes.BatchInfo, startHeight int64, endHeight int64) error {
	if lp.submitter == "" {
		return errors.New("submitter is not provided")
	}

	timer := time.NewTicker(time.Duration(lp.cfg.FetchInterval) * time.Millisecond)
	defer timer.Stop()

	page := 1
	height := startHeight
	nextHeight := height + int64(lp.cfg.BatchChainQueryHeightInterval)

LOOP:
	for {
		select {
		case <-ctx.Done():
			lp.logger.Info("Closing batch fetcher")
			return nil
		case <-timer.C:
			if isEnd, err := lp.fetchBatch(ctx, batchCh, page, height, nextHeight); err != nil {
				lp.logger.Debug("Failed fetching batch", "height", height, "page", page, "error", err)
				continue LOOP
			} else if isEnd {
				height = nextHeight
				nextHeight = height + int64(lp.cfg.BatchChainQueryHeightInterval)
				if height > endHeight {
					break LOOP
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
	return nil
}

func (lp L1Provider) fetchBatch(ctx context.Context, batchCh chan<- rstypes.BatchInfo, page int, height int64, nextHeight int64) (bool, error) {
	txsPerPage := int(lp.cfg.TxsPerPage)
	queryStr := fmt.Sprintf("tx.height >= %d AND tx.height < %d AND message.action='/opinit.ophost.v1.MsgRecordBatch' AND message.sender='%s'", height, nextHeight, lp.submitter)
	res, err := lp.client.TxSearch(ctx, queryStr, false, &page, &txsPerPage, "asc")
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

			batchCh <- rstypes.BatchInfo{
				Batch: msg.BatchBytes,
			}
		}
	}

	if res.TotalCount <= page*txsPerPage {
		return true, nil
	}
	return false, nil
}

func (lp L1Provider) GetLatestFinalizedBlock(ctx context.Context) (uint64, error) {
	var reqMsg ophostv1.QueryLastFinalizedOutputRequest
	reqMsg.BridgeId = uint64(lp.cfg.BridgeID)
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

func (lp L1Provider) GetBatchInfoUpdates(ctx context.Context) (rstypes.BatchInfoUpdates, error) {
	batchInfoUpdates := make(rstypes.BatchInfoUpdates, 0)
	page := 1
	txsPerPage := int(lp.cfg.TxsPerPage)

	queryStr := fmt.Sprintf("create_bridge.bridge_id='%d'", lp.cfg.BridgeID)
	res, err := lp.client.TxSearch(ctx, queryStr, false, &page, &txsPerPage, "asc")
	if err != nil {
		return nil, err
	}
	for _, tx := range res.Txs {
		messages, err := unmarshalCosmosTx(tx.Tx)
		if err != nil {
			return nil, err
		}
		for _, anyMsg := range messages {
			if anyMsg.TypeUrl != "/opinit.ophost.v1.MsgCreateBridge" {
				continue
			}
			msg := &ophostv1.MsgCreateBridge{}

			err := anyMsg.UnmarshalTo(msg)
			if err != nil {
				return nil, err
			}

			batchInfoUpdates = append(batchInfoUpdates, rstypes.BatchInfoUpdate{
				Chain:     msg.Config.BatchInfo.Chain,
				Submitter: msg.Config.BatchInfo.Submitter,
				Start:     1,
			})
		}
	}

	blocksPerPage := int(lp.cfg.BlocksPerPage)
	queryStr = fmt.Sprintf("update_batch_info.bridge_id='%d'", lp.cfg.BridgeID)
	for {
		res, err := lp.client.BlockSearch(ctx, queryStr, &page, &blocksPerPage, "asc")
		if err != nil {
			return nil, err
		}
		for _, block := range res.Blocks {
			blockResult, err := lp.client.BlockResults(ctx, &block.Block.Height)
			if err != nil {
				return nil, err
			}

			for _, event := range blockResult.FinalizeBlockEvents {
				if event.Type == "update_batch_info" {
					batchInfoUpdate := rstypes.BatchInfoUpdate{}
					for _, attr := range event.Attributes {
						switch attr.Key {
						case "batch_chain":
							batchInfoUpdate.Chain = attr.Value
						case "batch_submitter":
							batchInfoUpdate.Submitter = attr.Value
						case "finalized_l2_block_number":
							l2BlockNumber, err := strconv.ParseInt(attr.Value, 10, 64)
							if err != nil {
								return nil, err
							}
							batchInfoUpdate.Start = l2BlockNumber + 1
						}
					}

					batchInfoUpdates[len(batchInfoUpdates)-1].End = batchInfoUpdate.Start - 1
					batchInfoUpdates = append(batchInfoUpdates, batchInfoUpdate)
				}
			}
		}

		if res.TotalCount <= page*txsPerPage {
			break
		}
		page++
	}
	return batchInfoUpdates, nil
}
