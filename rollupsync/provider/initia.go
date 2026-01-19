package provider

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	rpcclient "github.com/cometbft/cometbft/rpc/client"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"

	gogoproto "github.com/cosmos/gogoproto/proto"
	"google.golang.org/protobuf/proto"

	v1beta1 "cosmossdk.io/api/cosmos/base/query/v1beta1"
	rstypes "github.com/cometbft/cometbft/rollupsync/types"
	opchildv1 "github.com/initia-labs/OPinit/api/opinit/opchild/v1"
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
	} else if len(resBlock.Block.Txs) == 0 {
		return nil, fmt.Errorf("no tx found at height %d", height)
	}
	return resBlock.Block.Txs[0], nil
}

func (lp L1Provider) GetValidators(ctx context.Context, height int64, page int, perPage int) (*coretypes.ResultValidators, error) {
	res, err := lp.client.Validators(ctx, &height, &page, &perPage)
	if err != nil {
		return nil, err
	} else if res.Total == 0 {
		return nil, fmt.Errorf("no validators found at height %d", height)
	}
	return res, nil
}

func (lp L1Provider) GetBlock(ctx context.Context, height int64) (*cmttypes.Block, error) {
	resBlock, err := lp.client.Block(ctx, &height)
	if err != nil {
		return nil, err
	} else if resBlock.Block == nil || resBlock.Block.Height == 0 {
		return nil, fmt.Errorf("no block found at height %d", height)
	}
	return resBlock.Block, nil
}

func (lp L1Provider) GetHeader(ctx context.Context, height int64) (*cmttypes.Header, error) {
	resHeader, err := lp.client.Header(ctx, &height)
	if err != nil {
		return nil, err
	} else if resHeader.Header == nil || resHeader.Header.Height == 0 {
		return nil, fmt.Errorf("no header found at height %d", height)
	}
	return resHeader.Header, nil
}

// GetOraclePriceHashWithProof queries the oracle price hash from L1 ophost with Merkle proof
func (lp L1Provider) GetOraclePriceHashWithProof(ctx context.Context, height int64) ([]byte, []byte, error) {
	// Key: OraclePriceHashPrefix = 0xa1
	key := []byte{0xa1}

	res, err := lp.client.ABCIQueryWithOptions(ctx,
		"/store/ophost/key",
		key,
		rpcclient.ABCIQueryOptions{
			Height: height,
			Prove:  true,
		})
	if err != nil {
		return nil, nil, err
	}
	if res.Response.Code != 0 {
		return nil, nil, errors.New(res.Response.Log)
	}

	proofBytes, err := res.Response.ProofOps.Marshal()
	if err != nil {
		return nil, nil, err
	}

	return res.Response.Value, proofBytes, nil
}

// GetAllCurrencyPairs queries all currency pairs from L1 oracle module
func (lp L1Provider) GetAllCurrencyPairs(ctx context.Context, height int64) ([]string, error) {
	res, err := lp.client.ABCIQueryWithOptions(ctx,
		"/connect.oracle.v2.Query/GetAllCurrencyPairs",
		[]byte{},
		rpcclient.ABCIQueryOptions{Height: height})
	if err != nil {
		return nil, err
	}
	if res.Response.Code != 0 {
		return nil, errors.New(res.Response.Log)
	}

	resp := new(rstypes.GetAllCurrencyPairsResponse)
	if err := gogoproto.Unmarshal(res.Response.Value, resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal currency pairs response: %v", err)
	}

	pairs := make([]string, 0, len(resp.CurrencyPairs))
	for _, cp := range resp.CurrencyPairs {
		pairs = append(pairs, cp.Base+"/"+cp.Quote)
	}
	return pairs, nil
}

// GetOraclePrice queries a single currency pair price from L1 oracle module
func (lp L1Provider) GetOraclePrice(ctx context.Context, height int64, currencyPair string) (*opchildv1.OraclePriceData, error) {
	req := &rstypes.GetPriceRequest{CurrencyPair: currencyPair}
	reqBytes, err := gogoproto.Marshal(req)
	if err != nil {
		return nil, err
	}

	res, err := lp.client.ABCIQueryWithOptions(ctx,
		"/connect.oracle.v2.Query/GetPrice",
		reqBytes,
		rpcclient.ABCIQueryOptions{Height: height})
	if err != nil {
		return nil, err
	}
	if res.Response.Code != 0 {
		return nil, errors.New(res.Response.Log)
	}

	resp := new(rstypes.GetPriceResponse)
	if err := gogoproto.Unmarshal(res.Response.Value, resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal price response: %v", err)
	}
	if resp.Price == nil {
		return nil, fmt.Errorf("no price data for currency pair %s", currencyPair)
	}

	return &opchildv1.OraclePriceData{
		CurrencyPair:   currencyPair,
		Price:          resp.Price.Price,
		Decimals:       resp.Decimals,
		Nonce:          resp.Nonce,
		CurrencyPairId: resp.Id,
		Timestamp:      resp.Price.BlockTimestamp.UnixNano(),
	}, nil
}
