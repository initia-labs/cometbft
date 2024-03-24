package provider

import (
	"strings"

	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
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
