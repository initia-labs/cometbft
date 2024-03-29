package types

import "context"

type BatchProvider interface {
	BatchFetcher(context.Context, int64, int64) error
	GetBatchChannel() <-chan BatchInfo
	GetLastHeight(context.Context) (int64, error)
	Quit()
}

type OutputProvider interface {
	GetLatestFinalizedBlock(context.Context) (uint64, error)
}
