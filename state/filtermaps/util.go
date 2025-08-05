package filtermaps

import (
	"fmt"

	abci "github.com/cometbft/cometbft/abci/types"
)

type TxEvent struct {
	BlockNumber int64
	TxIndex     int
}

func EventString(eventType string, eventAttribute abci.EventAttribute) string {
	return fmt.Sprintf("%s.%s=%s", eventType, eventAttribute.Key, eventAttribute.Value)
}
