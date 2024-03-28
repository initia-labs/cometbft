package types

import "context"

type BatchProvider interface {
	BatchFetcher(context.Context, int64, int64) error
	GetBatchChannel() <-chan []byte
	Quit()
}

type OutputProvider interface {
	GetLatestFinalizedBlock(context.Context) (uint64, error)
	GetQueryHeightRange(context.Context) (int64, int64, error)
}
