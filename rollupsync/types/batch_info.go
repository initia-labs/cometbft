package types

import "fmt"

type BatchInfoUpdates []BatchInfoUpdate

func (b BatchInfoUpdates) String() string {
	res := ""
	for _, update := range b {
		res += fmt.Sprintf("| %d ~ %d: %s, %s ", update.Start, update.End, update.Chain, update.Submitter)
	}
	return res
}

type BatchInfoUpdate struct {
	Chain     string
	Submitter string
	Start     int64
	End       int64
}
