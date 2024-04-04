package types

import "context"

type BatchProvider interface {
	BatchFetcher(context.Context, chan<- BatchChanInfo, int64, int64) error
	GetLastHeight(context.Context) (int64, error)
	SetSubmitter(string)
}
