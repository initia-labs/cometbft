package types

import "context"

type BatchProvider interface {
	BatchFetcher(context.Context, chan<- BatchChanInfo, int64, *uint64) error
	GetLastHeight(context.Context) (int64, error)
	FirstTxHeight(ctx context.Context) (int64, error)
	SetSubmitter(string)
}
