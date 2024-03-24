package types

import "context"

type BatchProvider interface {
	BatchFetcher(context.Context) error
	GetBatchChannel() <-chan []byte
}

type OutputProvider interface {
	GetLatestFinalizedBlock(context.Context) (uint64, error)
}
