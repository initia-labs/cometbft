package core

import (
	"errors"

	ctypes "github.com/cometbft/cometbft/rpc/core/types"
	rpctypes "github.com/cometbft/cometbft/rpc/jsonrpc/types"
)

func (env *Environment) AttestorPubKey(
	_ *rpctypes.Context,
) (*ctypes.ResultAttestorPubKey, error) {
	if env.NodeKey == nil {
		return nil, errors.New("attestation is not supported")
	}
	return &ctypes.ResultAttestorPubKey{PubKey: env.NodeKey.PubKey().Bytes()}, nil
}
