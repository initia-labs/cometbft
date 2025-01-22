package provider

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	"google.golang.org/protobuf/proto"

	v1beta1 "cosmossdk.io/api/cosmos/base/query/v1beta1"
	rstypes "github.com/cometbft/cometbft/rollupsync/types"
	ophostv1 "github.com/initia-labs/OPinit/api/opinit/ophost/v1"

	cmttypes "github.com/cometbft/cometbft/types"
)

type L1Provider struct {
	logger log.Logger
	cfg    *config.RollupSyncConfig
	client *rpchttp.HTTP
}

func NewL1Provider(logger log.Logger, cfg *config.RollupSyncConfig) (*L1Provider, error) {
	idx := slices.IndexFunc(cfg.RPCServers, func(elem config.RollupSyncRPCConfig) bool {
		return elem.Chain == rstypes.ChainNameL1
	})
	if idx < 0 {
		return nil, fmt.Errorf("%s rpc address is not provided", rstypes.ChainNameL1)
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

		if msg.Pagination.Total <= 100 {
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

func (lp L1Provider) GetAllValidators(ctx context.Context, height int64) ([]*cmttypes.Validator, error) {
	validators := make([]*cmttypes.Validator, 0)
	page := 1
	perPage := 100
	for {
		res, err := lp.client.Validators(ctx, &height, &page, &perPage)
		if err != nil {
			return nil, err
		}
		validators = append(validators, res.Validators...)

		if len(validators) == res.Total {
			break
		}
		page++
	}
	return validators, nil
}

func (lp L1Provider) GetBlock(ctx context.Context, height int64) (*cmttypes.Block, error) {
	resBlock, err := lp.client.Block(ctx, &height)
	if err != nil {
		return nil, err
	}
	return resBlock.Block, nil
}

func (lp L1Provider) GetHeader(ctx context.Context, height int64) (*cmttypes.Header, error) {
	resHeader, err := lp.client.Header(ctx, &height)
	if err != nil {
		return nil, err
	}
	return resHeader.Header, nil
}
