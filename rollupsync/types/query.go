package types

import (
	"fmt"

	ophostv1 "github.com/initia-labs/OPinit/api/opinit/ophost/v1"
)

func QueryEventTypeWithSubmitterFromChainType(chainType ophostv1.BatchInfo_ChainType, submitter string) string {
	switch chainType {
	case ophostv1.BatchInfo_INITIA:
		return fmt.Sprintf("record_batch.submitter='%s'", submitter)
	case ophostv1.BatchInfo_CELESTIA:
		return fmt.Sprintf("message.sender='%s'", submitter)
	}
	return ""
}
