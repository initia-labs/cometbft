package rollupsync

import (
	"bytes"
	"context"
	"errors"
	"time"

	authzv1beta1 "cosmossdk.io/api/cosmos/authz/v1beta1"
	ibcprotoclient "github.com/cometbft/cometbft/proto/ibc/core/client/v1"
	ibcprotopmlcs "github.com/cometbft/cometbft/proto/ibc/lightclients/tendermint/v1"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/rollupsync/provider"
	"github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-proto/anyutil"
	opchildv1 "github.com/initia-labs/OPinit/api/opinit/opchild/v1"
	"google.golang.org/protobuf/proto"
)

func (rs *RollupSyncer) fillData(ctx context.Context, block *types.Block) error {
	for i, txBytes := range block.Txs {
		raw, body, err := provider.UnmarshalCosmosTx(txBytes)
		if err != nil {
			return err
		}

		for _, anyMsg := range body.Messages {
			switch anyMsg.TypeUrl {
			case "/opinit.opchild.v1.MsgUpdateOracle":
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
			case "/cosmos.authz.v1beta1.MsgExec":
				authzMsg := new(authzv1beta1.MsgExec)
				err := anyMsg.UnmarshalTo(authzMsg)
				if err != nil {
					return err
				}
				if len(authzMsg.Msgs) != 1 || authzMsg.Msgs[0].TypeUrl != "/opinit.opchild.v1.MsgUpdateOracle" {
					continue
				}
				msg := new(opchildv1.MsgUpdateOracle)
				err = authzMsg.Msgs[0].UnmarshalTo(msg)
				if err != nil {
					return err
				}

				oracleTx, err := rs.fetchOracleTx(ctx, int64(msg.Height))
				if err != nil {
					return errors.Join(errors.New("failed to fetch oracle tx"), err)
				}
				msg.Data = oracleTx

				// https://github.com/cosmos/cosmos-sdk/blob/main/docs/learn/advanced/05-encoding.md#anys-typeurl
				err = anyutil.MarshalFrom(authzMsg.Msgs[0], msg, proto.MarshalOptions{})
				if err != nil {
					return errors.Join(errors.New("failed to marshal oracle msg"), err)
				}

				err = anyutil.MarshalFrom(anyMsg, authzMsg, proto.MarshalOptions{})
				if err != nil {
					return errors.Join(errors.New("failed to marshal oracle msg"), err)
				}
			case "/ibc.core.client.v1.MsgUpdateClient":
				updateClientMsg := new(ibcprotoclient.MsgUpdateClient)
				err := updateClientMsg.Unmarshal(anyMsg.Value)
				if err != nil {
					return err
				}

				if updateClientMsg.ClientMessage.TypeUrl != "/ibc.lightclients.tendermint.v1.Header" {
					continue
				}

				tmHeader := new(ibcprotopmlcs.Header)
				err = tmHeader.Unmarshal(updateClientMsg.ClientMessage.Value)
				if err != nil {
					return err
				}

				// fill ValidatorSet
				height := tmHeader.SignedHeader.Commit.Height
				validators, err := rs.getAllValidators(ctx, height)
				if err != nil {
					return err
				}
				cmtValidators, _, err := toCmtProtoValidators(validators)
				if err != nil {
					return err
				}
				if tmHeader.ValidatorSet == nil {
					tmHeader.ValidatorSet = new(cmtproto.ValidatorSet)
				}
				tmHeader.ValidatorSet.Validators = cmtValidators
				for _, val := range cmtValidators {
					if bytes.Equal(val.Address, tmHeader.SignedHeader.Header.ProposerAddress) {
						tmHeader.ValidatorSet.Proposer = val
					}
				}

				// fill TrustedValidators
				height = int64(tmHeader.TrustedHeight.RevisionHeight)
				validators, err = rs.getAllValidators(ctx, height)
				if err != nil {
					return err
				}
				cmtValidators, _, err = toCmtProtoValidators(validators)
				if err != nil {
					return err
				}
				blockHeader, err := rs.l1Provider.GetHeader(ctx, height)
				if err != nil {
					return err
				}
				if tmHeader.TrustedValidators == nil {
					tmHeader.TrustedValidators = new(cmtproto.ValidatorSet)
				}
				tmHeader.TrustedValidators.Validators = cmtValidators
				for _, val := range cmtValidators {
					if bytes.Equal(val.Address, blockHeader.ProposerAddress.Bytes()) {
						tmHeader.TrustedValidators.Proposer = val
					}
				}

				// fill commit signatures
				height = tmHeader.SignedHeader.Commit.Height + 1
				block, err := rs.l1Provider.GetBlock(ctx, height)
				if err != nil {
					return err
				}

				for sigIndex, signature := range tmHeader.SignedHeader.Commit.Signatures {
					if len(signature.Signature) == 2 {
						// fill signature
						blockSigIndex := int(signature.Signature[0]) + int(signature.Signature[1])<<8
						tmHeader.SignedHeader.Commit.Signatures[sigIndex] = *block.LastCommit.Signatures[blockSigIndex].ToProto()
					}
				}

				updateClientMsg.ClientMessage.Value, err = tmHeader.Marshal()
				if err != nil {
					return errors.Join(errors.New("failed to marshal tm header"), err)
				}

				anyMsg.Value, err = updateClientMsg.Marshal()
				if err != nil {
					return errors.Join(errors.New("failed to marshal update client msg"), err)
				}
			default:
				continue
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

func (rs *RollupSyncer) getAllValidators(ctx context.Context, height int64) ([]*types.Validator, error) {
	ticker := time.NewTicker(time.Duration(rs.cfg.FetchInterval) * time.Millisecond)
	defer ticker.Stop()

	retry := 0
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			validators, err := rs.l1Provider.GetAllValidators(ctx, height)
			if err != nil {
				rs.logger.Error("failed to fetch validators", "height", height, "retry", retry, "error", err.Error())
				retry++
				continue
			}
			return validators, nil
		}
	}
}

func toCmtProtoValidators(validators []*types.Validator) ([]*cmtproto.Validator, int, error) {
	cmtValidators := make([]*cmtproto.Validator, 0)
	for _, val := range validators {
		protoVal, err := val.ToProto()
		if err != nil {
			return nil, 0, err
		}
		cmtValidators = append(cmtValidators, protoVal)
	}
	return cmtValidators, len(validators), nil
}
