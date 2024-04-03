package provider

import (
	"errors"
	"strings"

	abciv1beta1 "cosmossdk.io/api/cosmos/base/abci/v1beta1"
	txv1beta1 "cosmossdk.io/api/cosmos/tx/v1beta1"
	"google.golang.org/protobuf/proto"
	anypb "google.golang.org/protobuf/types/known/anypb"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	celblob "github.com/celestiaorg/go-square/blob"
)

// RPCClient sets up a new RPC client
func RPCClient(server string) (*rpchttp.HTTP, error) {
	if !strings.Contains(server, "://") {
		server = "http://" + server
	}

	c, err := rpchttp.New(server, "/websocket")
	if err != nil {
		return nil, err
	}
	return c, nil
}

func unmarshalCosmosTxData(data []byte) ([]*anypb.Any, error) {
	var msgData abciv1beta1.TxMsgData
	if err := proto.Unmarshal(data, &msgData); err != nil {
		return nil, err
	}
	return msgData.MsgResponses, nil
}

func unmarshalCosmosTx(txbytes []byte) ([]*anypb.Any, error) {
	var raw txv1beta1.TxRaw
	if err := proto.Unmarshal(txbytes, &raw); err != nil {
		return nil, err
	}

	var body txv1beta1.TxBody
	if err := proto.Unmarshal(raw.BodyBytes, &body); err != nil {
		return nil, err
	}
	return body.Messages, nil
}

func unmarshalCelestiaBlobTx(txbytes []byte) (*celblob.BlobTx, error) {
	blobTx, success := celblob.UnmarshalBlobTx(txbytes)
	if !success {
		return nil, errors.New("fail unmarshaling celestia blobtx")
	}
	return blobTx, nil
}
