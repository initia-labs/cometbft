package engine

import (
	"github.com/go-kit/kit/metrics"

	cmttypes "github.com/cometbft/cometbft/types"
)

const (
	// MetricsSubsystem is the Prometheus subsystem under which engine metrics are exposed.
	MetricsSubsystem = "sequencing_engine"
)

//go:generate go run ../../scripts/metricsgen -struct=Metrics

// Metrics contains broadcast and block execution metrics for the sequencing engine.
type Metrics struct {
	StatusBroadcasts          metrics.Counter
	StatusBroadcastFailures   metrics.Counter
	AttestBroadcasts          metrics.Counter
	AttestBroadcastFailures   metrics.Counter
	ProposalBroadcasts        metrics.Counter
	ProposalBroadcastFailures metrics.Counter

	// Whether or not the sequencing engine is running. 1 if yes, 0 if no.
	Syncing metrics.Gauge
	// Number of transactions in the latest block.
	NumTxs metrics.Gauge
	// Total number of transactions processed by the sequencing engine.
	TotalTxs metrics.Gauge
	// Size of the latest block in bytes.
	BlockSizeBytes metrics.Gauge
	// Height of the latest applied block.
	LatestBlockHeight metrics.Gauge
}

func (m *Metrics) recordBlockMetrics(block *cmttypes.Block) {
	if m == nil || block == nil {
		return
	}

	m.NumTxs.Set(float64(len(block.Data.Txs)))
	m.TotalTxs.Add(float64(len(block.Data.Txs)))
	m.BlockSizeBytes.Set(float64(block.Size()))
	m.LatestBlockHeight.Set(float64(block.Height))
}
