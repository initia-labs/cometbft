package provider

import (
	"errors"
	"strings"

	txv1beta1 "cosmossdk.io/api/cosmos/tx/v1beta1"
	"google.golang.org/protobuf/proto"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	celblob "github.com/celestiaorg/go-square/blob"
)

// newRpcClient sets up a new RPC client
func newRpcClient(server string) (*rpchttp.HTTP, error) {
	if !strings.Contains(server, "://") {
		server = "http://" + server
	}

	c, err := rpchttp.New(server, "/websocket")
	if err != nil {
		return nil, err
	}
	return c, nil
}

func UnmarshalCosmosTx(txbytes []byte) (*txv1beta1.TxRaw, *txv1beta1.TxBody, error) {
	var raw txv1beta1.TxRaw
	if err := proto.Unmarshal(txbytes, &raw); err != nil {
		return nil, nil, err
	}

	var body txv1beta1.TxBody
	if err := proto.Unmarshal(raw.BodyBytes, &body); err != nil {
		return nil, nil, err
	}
	return &raw, &body, nil
}

func MarshalCosmosTx(raw *txv1beta1.TxRaw, body *txv1beta1.TxBody) ([]byte, error) {
	bodyBytes, err := proto.Marshal(body)
	if err != nil {
		return nil, err
	}
	raw.BodyBytes = bodyBytes
	rawBytes, err := proto.Marshal(raw)
	if err != nil {
		return nil, err
	}
	return rawBytes, nil
}

func unmarshalCelestiaBlobTx(txbytes []byte) (*celblob.BlobTx, error) {
	blobTx, success := celblob.UnmarshalBlobTx(txbytes)
	if !success {
		return nil, errors.New("fail unmarshaling celestia blobtx")
	}
	return blobTx, nil
}
