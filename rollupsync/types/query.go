package types

import (
	"fmt"

	ophostv1 "github.com/initia-labs/OPinit/api/opinit/ophost/v1"
)

func QueryEventTypeWithSubmitterFromChainType(chainType ophostv1.BatchInfo_ChainType, submitter string) string {
	switch chainType {
	case ophostv1.BatchInfo_CHAIN_TYPE_INITIA:
		return fmt.Sprintf("record_batch.submitter='%s'", submitter)
	case ophostv1.BatchInfo_CHAIN_TYPE_CELESTIA:
		return fmt.Sprintf("message.sender='%s' AND message.action='/celestia.blob.v1.MsgPayForBlobs'", submitter)
	}
	return ""
}
