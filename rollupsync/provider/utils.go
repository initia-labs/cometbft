package provider

import (
	"errors"
	"strings"

	v1beta1 "cosmossdk.io/api/cosmos/tx/v1beta1"
	"google.golang.org/protobuf/proto"
	anypb "google.golang.org/protobuf/types/known/anypb"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"

	celblob "github.com/celestiaorg/go-square/blob"
)

var (
	DEFAULT_FETCH_INTERVAL  = 10 // millisecond
	DEFAULT_TXS_PER_PAGE    = 100
	DEFAULT_HEIGHT_INTERVAL = 1000
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

func unmarshalCosmosTx(txbytes []byte) ([]*anypb.Any, error) {
	var raw v1beta1.TxRaw
	if err := proto.Unmarshal(txbytes, &raw); err != nil {
		return nil, err
	}

	var body v1beta1.TxBody
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
