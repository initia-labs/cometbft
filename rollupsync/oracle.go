package rollupsync

import (
	"context"
	"errors"
	"time"

	"github.com/cometbft/cometbft/rollupsync/provider"
	"github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-proto/anyutil"
	opchildv1 "github.com/initia-labs/OPinit/api/opinit/opchild/v1"
	"google.golang.org/protobuf/proto"
)

func (rs *RollupSyncer) fillOracleData(ctx context.Context, block *types.Block) error {
	for i, txBytes := range block.Txs {
		raw, body, err := provider.UnmarshalCosmosTx(txBytes)
		if err != nil {
			return err
		}

		for _, anyMsg := range body.Messages {
			if anyMsg.TypeUrl != "/opinit.opchild.v1.MsgUpdateOracle" {
				continue
			}

			msg := new(opchildv1.MsgUpdateOracle)
			err := anyMsg.UnmarshalTo(msg)
			if err != nil {
				return err
			}

			oracleTx, err := rs.fetchOracleTx(ctx, int64(msg.Height))
			if err != nil {
				return errors.Join(errors.New("failed to fetch oracle tx"), err)
			}
			msg.Data = oracleTx

			// https://github.com/cosmos/cosmos-sdk/blob/main/docs/learn/advanced/05-encoding.md#anys-typeurl
			err = anyutil.MarshalFrom(anyMsg, msg, proto.MarshalOptions{})
			if err != nil {
				return errors.Join(errors.New("failed to marshal oracle msg"), err)
			}
		}

		convertedTxBytes, err := provider.MarshalCosmosTx(raw, body)
		if err != nil {
			return errors.Join(errors.New("failed to marshal cosmos tx"), err)
		}
		block.Txs[i] = convertedTxBytes
	}
	return nil
}

func (rs *RollupSyncer) fetchOracleTx(ctx context.Context, height int64) ([]byte, error) {
	ticker := time.NewTicker(time.Duration(rs.cfg.FetchInterval) * time.Millisecond)
	defer ticker.Stop()

	retry := 0
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			oracleTx, err := rs.l1Provider.GetOracleTx(ctx, height)
			if err != nil {
				rs.logger.Error("failed to fetch oracle tx", "height", height, "retry", retry, "error", err.Error())
				retry++
				continue
			}
			return oracleTx, nil
		}
	}
}
