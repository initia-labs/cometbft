package core

import (
	"context"
	"errors"
	"fmt"
	"slices"

	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/libs/bytes"
	tmcrypto "github.com/cometbft/cometbft/proto/tendermint/crypto"
	tmibccommitmentproto "github.com/cometbft/cometbft/proto/tmibc/core/commitment/v1"
	"github.com/cometbft/cometbft/proxy"
	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	rpctypes "github.com/cometbft/cometbft/rpc/jsonrpc/types"

	"regexp"

	ics23 "github.com/cosmos/ics23/go"
)

// ABCIQuery queries the application for some information.
// More: https://docs.cometbft.com/v0.38.x/rpc/#/ABCI/abci_query
func (env *Environment) ABCIQuery(
	_ *rpctypes.Context,
	path string,
	data bytes.HexBytes,
	height int64,
	prove bool,
) (*ctypes.ResultABCIQuery, error) {
	resQuery, err := env.ProxyAppQuery.Query(context.TODO(), &abci.RequestQuery{
		Path:   path,
		Data:   data,
		Height: height,
		Prove:  prove,
	})
	if err != nil {
		return nil, err
	}

	return &ctypes.ResultABCIQuery{Response: *resQuery}, nil
}

func (env *Environment) ABCIQueryWithAttestation(
	_ *rpctypes.Context,
	path string,
	data bytes.HexBytes,
	height int64,
) (*ctypes.ResultABCIQueryWithAttestation, error) {
	if env.NodeKey == nil {
		return nil, errors.New("attestation is not supported")
	} else if slices.ContainsFunc(env.DisabledProofKeys, func(disabledKey *regexp.Regexp) bool {
		return disabledKey.Match(data.Bytes())
	}) {
		return nil, fmt.Errorf("this path is disabled to be proofed, path: %s", string(data.Bytes()))
	}

	resQuery, err := env.ProxyAppQuery.Query(context.TODO(), &abci.RequestQuery{
		Path:   path,
		Data:   data,
		Height: height,
		Prove:  true,
	})
	if err != nil {
		return nil, err
	} else if resQuery.ProofOps == nil {
		return nil, errors.New("no proof ops")
	}

	signature, err := SignProofs(env.NodeKey, resQuery.ProofOps)
	if err != nil {
		return nil, err
	}
	return &ctypes.ResultABCIQueryWithAttestation{Response: *resQuery, Attestation: signature, PubKey: env.NodeKey.PubKey().Bytes()}, nil
}

// ABCIInfo gets some info about the application.
// More: https://docs.cometbft.com/v0.38.x/rpc/#/ABCI/abci_info
func (env *Environment) ABCIInfo(_ *rpctypes.Context) (*ctypes.ResultABCIInfo, error) {
	resInfo, err := env.ProxyAppQuery.Info(context.TODO(), proxy.RequestInfo)
	if err != nil {
		return nil, err
	}

	return &ctypes.ResultABCIInfo{Response: *resInfo}, nil
}

func SignProofs(nodeKey crypto.PrivKey, proofs *tmcrypto.ProofOps) ([]byte, error) {
	merkleProof, err := convertProofs(proofs)
	if err != nil {
		return nil, err
	}
	merkleProofBz, err := merkleProof.Marshal()
	if err != nil {
		return nil, err
	}
	// TODO: check if the challenger has validated the height of the proof and there is no any challenges related to outputs.
	return nodeKey.Sign(merkleProofBz)
}

func convertProofs(tmProof *tmcrypto.ProofOps) (tmibccommitmentproto.MerkleProof, error) {
	if tmProof == nil {
		return tmibccommitmentproto.MerkleProof{}, errors.New("tendermint proof is nil")
	}
	// Unmarshal all proof ops to CommitmentProof
	proofs := make([]*ics23.CommitmentProof, len(tmProof.Ops))
	for i, op := range tmProof.Ops {
		var p ics23.CommitmentProof
		err := p.Unmarshal(op.Data)
		if err != nil || p.Proof == nil {
			return tmibccommitmentproto.MerkleProof{}, fmt.Errorf("could not unmarshal proof op into CommitmentProof at index %d: %v", i, err)
		}
		proofs[i] = &p
	}
	return tmibccommitmentproto.MerkleProof{
		Proofs: proofs,
	}, nil
}
