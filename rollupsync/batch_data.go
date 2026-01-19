package rollupsync

import (
	"bytes"
	"context"
	"errors"
	"math"

	authzv1beta1 "cosmossdk.io/api/cosmos/authz/v1beta1"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	ibcprotoclient "github.com/cometbft/cometbft/proto/tmibc/core/client/v1"
	ibcprotopmlcs "github.com/cometbft/cometbft/proto/tmibc/lightclients/tendermint/v1"
	"github.com/cometbft/cometbft/rollupsync/provider"
	"github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-proto/anyutil"
	opchildv1 "github.com/initia-labs/OPinit/api/opinit/opchild/v1"

	"google.golang.org/protobuf/proto"
)

// Note: due to the bug in GoRelayer that incorrectly sets the proposer,
// when proposerWithLowestPriority is true, we need to fill the proposer with the lowest priority.
func (rs *RollupSyncer) fillData(ctx context.Context, block *types.Block, proposerWithLowestPriority bool) error {
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

				err = SleepWithRetry(ctx, rs.cfg.FetchInterval, func(retry int) bool {
					res, err := rs.l1Provider.GetOracleTx(ctx, int64(msg.Height))
					if err != nil {
						rs.logger.Error("failed to fetch oracle tx", "height", msg.Height, "retry", retry, "error", err.Error())
						return false
					}
					msg.Data = res
					return true
				})
				if err != nil {
					return errors.Join(errors.New("failed to fetch oracle tx"), err)
				}

				// https://github.com/cosmos/cosmos-sdk/blob/main/docs/learn/advanced/05-encoding.md#anys-typeurl
				err = anyutil.MarshalFrom(anyMsg, msg, proto.MarshalOptions{})
				if err != nil {
					return errors.Join(errors.New("failed to marshal oracle msg"), err)
				}
			case "/opinit.opchild.v1.MsgRelayOracleData":
				msg := new(opchildv1.MsgRelayOracleData)
				if err := anyMsg.UnmarshalTo(msg); err != nil {
					return err
				}

				if err := SleepWithRetry(ctx, rs.cfg.FetchInterval, func(retry int) bool {
					pairs, err := rs.l1Provider.GetAllCurrencyPairs(ctx, int64(msg.OracleData.L1BlockHeight))
					if err != nil {
						rs.logger.Error("failed to get currency pairs", "retry", retry, "error", err.Error())
						return false
					}
					prices := make([]*opchildv1.OraclePriceData, 0, len(pairs))
					for _, pair := range pairs {
						priceData, err := rs.l1Provider.GetOraclePrice(ctx, int64(msg.OracleData.L1BlockHeight), pair)
						if err != nil {
							rs.logger.Debug("failed to get oracle price, skipping", "pair", pair, "error", err.Error())
							continue
						}
						prices = append(prices, priceData)
					}
					msg.OracleData.Prices = prices
					return true
				}); err != nil {
					return errors.Join(errors.New("failed to restore oracle prices"), err)
				}

				if err := SleepWithRetry(ctx, rs.cfg.FetchInterval, func(retry int) bool {
					proofHeight := int64(msg.OracleData.ProofHeight.RevisionHeight) - 1
					_, proofBytes, err := rs.l1Provider.GetOraclePriceHashWithProof(ctx, proofHeight)
					if err != nil {
						rs.logger.Error("failed to get oracle proof", "retry", retry, "error", err.Error())
						return false
					}
					msg.OracleData.Proof = proofBytes
					return true
				}); err != nil {
					return errors.Join(errors.New("failed to restore oracle proof"), err)
				}

				err = anyutil.MarshalFrom(anyMsg, msg, proto.MarshalOptions{})
				if err != nil {
					return errors.Join(errors.New("failed to marshal relay oracle msg"), err)
				}
			case "/cosmos.authz.v1beta1.MsgExec":
				authzMsg := new(authzv1beta1.MsgExec)
				if err := anyMsg.UnmarshalTo(authzMsg); err != nil {
					return err
				}

				modified := false
				for _, innerMsg := range authzMsg.Msgs {
					switch innerMsg.TypeUrl {
					case "/opinit.opchild.v1.MsgUpdateOracle":
						msg := new(opchildv1.MsgUpdateOracle)
						if err := innerMsg.UnmarshalTo(msg); err != nil {
							return err
						}

						if err := SleepWithRetry(ctx, rs.cfg.FetchInterval, func(retry int) bool {
							res, err := rs.l1Provider.GetOracleTx(ctx, int64(msg.Height))
							if err != nil {
								rs.logger.Error("failed to fetch oracle tx", "height", msg.Height, "retry", retry, "error", err.Error())
								return false
							}
							msg.Data = res
							return true
						}); err != nil {
							return errors.Join(errors.New("failed to fetch oracle tx"), err)
						}

						// https://github.com/cosmos/cosmos-sdk/blob/main/docs/learn/advanced/05-encoding.md#anys-typeurl
						if err := anyutil.MarshalFrom(innerMsg, msg, proto.MarshalOptions{}); err != nil {
							return errors.Join(errors.New("failed to marshal oracle msg"), err)
						}
						modified = true
					case "/opinit.opchild.v1.MsgRelayOracleData":
						msg := new(opchildv1.MsgRelayOracleData)
						if err := innerMsg.UnmarshalTo(msg); err != nil {
							return err
						}

						if err := SleepWithRetry(ctx, rs.cfg.FetchInterval, func(retry int) bool {
							pairs, err := rs.l1Provider.GetAllCurrencyPairs(ctx, int64(msg.OracleData.L1BlockHeight))
							if err != nil {
								rs.logger.Error("failed to get currency pairs", "retry", retry, "error", err.Error())
								return false
							}
							prices := make([]*opchildv1.OraclePriceData, 0, len(pairs))
							for _, pair := range pairs {
								priceData, err := rs.l1Provider.GetOraclePrice(ctx, int64(msg.OracleData.L1BlockHeight), pair)
								if err != nil {
									rs.logger.Debug("failed to get oracle price, skipping", "pair", pair, "error", err.Error())
									continue
								}
								prices = append(prices, priceData)
							}
							msg.OracleData.Prices = prices
							return true
						}); err != nil {
							return errors.Join(errors.New("failed to restore oracle prices"), err)
						}

						if err := SleepWithRetry(ctx, rs.cfg.FetchInterval, func(retry int) bool {
							proofHeight := int64(msg.OracleData.ProofHeight.RevisionHeight) - 1
							_, proofBytes, err := rs.l1Provider.GetOraclePriceHashWithProof(ctx, proofHeight)
							if err != nil {
								rs.logger.Error("failed to get oracle proof", "retry", retry, "error", err.Error())
								return false
							}
							msg.OracleData.Proof = proofBytes
							return true
						}); err != nil {
							return errors.Join(errors.New("failed to restore oracle proof"), err)
						}

						if err := anyutil.MarshalFrom(innerMsg, msg, proto.MarshalOptions{}); err != nil {
							return errors.Join(errors.New("failed to marshal relay oracle msg"), err)
						}
						modified = true
					}
				}

				if !modified {
					continue
				}

				if err := anyutil.MarshalFrom(anyMsg, authzMsg, proto.MarshalOptions{}); err != nil {
					return errors.Join(errors.New("failed to marshal authz msg"), err)
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
				if len(tmHeader.ValidatorSet.Validators) == 0 || proposerWithLowestPriority {
					totalVotingPower := tmHeader.ValidatorSet.TotalVotingPower
					tmHeader.ValidatorSet = new(cmtproto.ValidatorSet)

					height := tmHeader.SignedHeader.Commit.Height
					validators, err := rs.GetAllValidatorsWithRetry(ctx, height)
					if err != nil {
						return errors.Join(errors.New("failed to fetch validators"), err)
					}
					cmtValidators, _, err := toCmtProtoValidators(validators)
					if err != nil {
						return err
					}

					tmHeader.ValidatorSet.Validators = cmtValidators
					if proposerWithLowestPriority {
						tmHeader.ValidatorSet.Proposer = getProposerWithLowestPriority(cmtValidators)
					} else {
						for _, val := range cmtValidators {
							if bytes.Equal(val.Address, tmHeader.Header.ProposerAddress) {
								tmHeader.ValidatorSet.Proposer = val
							}
						}
					}
					tmHeader.ValidatorSet.TotalVotingPower = totalVotingPower
				}

				// fill TrustedValidators
				if len(tmHeader.TrustedValidators.Validators) == 0 || proposerWithLowestPriority {
					totalVotingPower := tmHeader.TrustedValidators.TotalVotingPower
					tmHeader.TrustedValidators = new(cmtproto.ValidatorSet)

					height := int64(tmHeader.TrustedHeight.RevisionHeight + 1)

					validators, err := rs.GetAllValidatorsWithRetry(ctx, height)
					if err != nil {
						return errors.Join(errors.New("failed to fetch validators"), err)
					}

					cmtValidators, _, err := toCmtProtoValidators(validators)
					if err != nil {
						return err
					}

					var blockHeader *types.Header
					err = SleepWithRetry(ctx, rs.cfg.FetchInterval, func(retry int) bool {
						res, err := rs.l1Provider.GetHeader(ctx, height)
						if err != nil {
							rs.logger.Error("failed to fetch block header", "height", height, "retry", retry, "error", err.Error())
							return false
						}
						blockHeader = res
						return true
					})
					if err != nil {
						return errors.Join(errors.New("failed to fetch block header"), err)
					}
					tmHeader.TrustedValidators.Validators = cmtValidators
					if proposerWithLowestPriority {
						tmHeader.TrustedValidators.Proposer = getProposerWithLowestPriority(cmtValidators)
					} else {
						for _, val := range cmtValidators {
							if bytes.Equal(val.Address, blockHeader.ProposerAddress.Bytes()) {
								tmHeader.TrustedValidators.Proposer = val
							}
						}
					}
					tmHeader.TrustedValidators.TotalVotingPower = totalVotingPower
				}

				// fill commit signatures
				height := tmHeader.SignedHeader.Commit.Height + 1

				var tmpBlock *types.Block
				err = SleepWithRetry(ctx, rs.cfg.FetchInterval, func(retry int) bool {
					res, err := rs.l1Provider.GetBlock(ctx, height)
					if err != nil {
						rs.logger.Error("failed to fetch block header", "height", height, "retry", retry, "error", err.Error())
						return false
					}
					tmpBlock = res
					return true
				})
				if err != nil {
					return errors.Join(errors.New("failed to fetch block header"), err)
				}

				for sigIndex, signature := range tmHeader.SignedHeader.Commit.Signatures {
					if len(signature.Signature) == 2 {
						// fill signature
						blockSigIndex := int(signature.Signature[0]) + int(signature.Signature[1])<<8
						tmHeader.SignedHeader.Commit.Signatures[sigIndex] = *tmpBlock.LastCommit.Signatures[blockSigIndex].ToProto()
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

func (rs *RollupSyncer) GetAllValidatorsWithRetry(ctx context.Context, height int64) ([]*types.Validator, error) {
	validators := make([]*types.Validator, 0)
	page := 1
	perPage := 100
	total := 0

	for {
		err := SleepWithRetry(ctx, rs.cfg.FetchInterval, func(retry int) bool {
			res, err := rs.l1Provider.GetValidators(ctx, height, page, perPage)
			if err != nil {
				rs.logger.Error("failed to fetch validators", "height", height, "retry", retry, "error", err.Error())
				return false
			}
			validators = append(validators, res.Validators...)
			total = res.Total
			return true
		})
		if err != nil {
			return nil, errors.Join(errors.New("failed to fetch validators"), err)
		}

		if total != 0 && len(validators) == total {
			break
		}
		page++
	}
	return validators, nil
}

func getProposerWithLowestPriority(validators []*cmtproto.Validator) *cmtproto.Validator {
	lowestPriority := int64(math.MaxInt64)
	var proposer *cmtproto.Validator
	for _, val := range validators {
		if val.ProposerPriority < lowestPriority {
			lowestPriority = val.ProposerPriority
			proposer = val
		}
	}
	return proposer
}
