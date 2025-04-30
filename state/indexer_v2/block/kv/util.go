package kv

import (
	"encoding/binary"
	"fmt"
	"math/big"

	idxutil "github.com/cometbft/cometbft/internal/indexer"
	"github.com/cometbft/cometbft/libs/pubsub/query/syntax"
	indexer "github.com/cometbft/cometbft/state/indexer_v2"
	"github.com/cometbft/cometbft/types"
)

type HeightInfo struct {
	heightRange     indexer.QueryRange
	height          int64
	heightEqIdx     int
	onlyHeightRange bool
	onlyHeightEq    bool
}

func int64FromBytes(bz []byte) int64 {
	v, _ := binary.Varint(bz)
	return v
}

func int64ToBytes(i int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, i)
	return buf[:n]
}

// Remove all occurrences of height equality queries except one. While we are traversing the conditions, check whether the only condition in
// addition to match events is the height equality or height range query. At the same time, if we do have a height range condition
// ignore the height equality condition. If a height equality exists, place the condition index in the query and the desired height
// into the heightInfo struct
func dedupHeight(conditions []syntax.Condition) (dedupConditions []syntax.Condition, heightInfo HeightInfo, err error) {
	heightInfo.heightEqIdx = -1
	heightRangeExists := false
	found := false
	var heightCondition []syntax.Condition
	heightInfo.onlyHeightEq = true
	heightInfo.onlyHeightRange = true
	for _, c := range conditions {
		if c.Tag == types.BlockHeightKey {
			if c.Op == syntax.TEq {
				if found || heightRangeExists {
					return nil, heightInfo, fmt.Errorf("invalid height configuration")
				}
				hFloat := c.Arg.Number()
				if hFloat != nil {
					h, _ := hFloat.Int64()
					heightInfo.height = h
					heightCondition = append(heightCondition, c)
					found = true
				}
			} else {
				if found {
					return nil, heightInfo, fmt.Errorf("invalid height configuration")
				}
				heightInfo.onlyHeightEq = false
				heightRangeExists = true
				dedupConditions = append(dedupConditions, c)
			}
		} else {
			heightInfo.onlyHeightRange = false
			heightInfo.onlyHeightEq = false
			dedupConditions = append(dedupConditions, c)
		}
	}
	if !heightRangeExists && len(heightCondition) != 0 {
		heightInfo.heightEqIdx = len(dedupConditions)
		heightInfo.onlyHeightRange = false
		dedupConditions = append(dedupConditions, heightCondition...)
	} else {
		// If we found a range make sure we set the hegiht idx to -1 as the height equality
		// will be removed
		heightInfo.heightEqIdx = -1
		heightInfo.height = 0
		heightInfo.onlyHeightEq = false
		found = false
	}
	return dedupConditions, heightInfo, nil
}

func checkHeightConditions(heightInfo HeightInfo, keyHeight int64) (bool, error) {
	if heightInfo.heightRange.Key != "" {
		withinBounds, err := idxutil.CheckBoundsV2(heightInfo.heightRange, big.NewInt(keyHeight))
		if err != nil || !withinBounds {
			return false, err
		}
	} else {
		if heightInfo.height != 0 && keyHeight != heightInfo.height {
			return false, nil
		}
	}
	return true, nil
}
