package rollupsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cometbft/cometbft/rollupsync/provider"
	rstypes "github.com/cometbft/cometbft/rollupsync/types"
	ophostv1 "github.com/initia-labs/OPinit/api/opinit/ophost/v1"
)

func (rs RollupSyncer) batchProvider(chainType ophostv1.BatchInfo_ChainType) (rstypes.BatchProvider, error) {
	switch chainType {
	case ophostv1.BatchInfo_CHAIN_TYPE_INITIA:
		return rs.l1Provider, nil
	case ophostv1.BatchInfo_CHAIN_TYPE_CELESTIA:
		return provider.NewCelestiaProvider(rs.logger.With("provider", rstypes.CHAIN_NAME_CELESTIA), rs.cfg)
	}

	return nil, errors.New("not implemented")
}

func (rs *RollupSyncer) batchFetcher(ctx context.Context) error {
	height := rs.state.LastBlockHeight + 1

	batchInfoMu := sync.Mutex{}
	batchInfos, err := rs.l1Provider.GetBatchInfos(ctx)
	if len(batchInfos) == 0 {
		return errors.New("no batch info")
	}

	batchInfoIndex := 0
	for i := range batchInfos {
		if i == len(batchInfos)-1 || batchInfos[i+1].Output.L2BlockNumber >= uint64(height) {
			batchInfoIndex = i
			break
		}
	}

	batchChainStartHeight, err := rs.blockExec.Store().GetRollupSyncBatchChainHeight(int64(batchInfoIndex))
	if err != nil {
		return err
	}
	batchChainStartHeight++

	batchInfoUpdater := func(index int, lastL2Height *uint64) {
		batchInfoUpdateTicker := time.NewTicker(time.Duration(rs.cfg.FetchInterval) * time.Millisecond)
		defer batchInfoUpdateTicker.Stop()

		for {
			select {
			case <-batchInfoUpdateTicker.C:
				nextInfo, err := rs.l1Provider.GetNextBatchInfo(ctx, uint64(index))
				if err != nil {
					continue
				}
				if nextInfo != nil {
					batchInfoMu.Lock()
					batchInfos = append(batchInfos, nextInfo)
					batchInfoMu.Unlock()
					*lastL2Height = nextInfo.Output.L2BlockNumber
					return
				}
			}
		}
	}

	for {
		batchInfo := batchInfos[batchInfoIndex]
		lastL2Height := uint64(0)

		if batchInfoIndex < len(batchInfos)-1 {
			lastL2Height = batchInfos[batchInfoIndex+1].Output.L2BlockNumber
		} else if batchInfoIndex == len(batchInfos)-1 {
			lastL2Height = rs.targetBlockHeight
			go batchInfoUpdater(batchInfoIndex, &lastL2Height)
		}

		batchProvider, err := rs.batchProvider(batchInfo.BatchInfo.ChainType)
		if err != nil {
			return err
		}
		batchProvider.SetSubmitter(batchInfo.BatchInfo.Submitter)
		rs.logger.Info("batch info", "start_l2_block_number", batchInfo.Output.L2BlockNumber+1, "chain", batchInfo.BatchInfo.ChainType, "submitter", batchInfo.BatchInfo.Submitter)
		rs.logger.Info("batch chain query range", "chain", batchInfo.BatchInfo.ChainType, "range", fmt.Sprintf("%d ~ ", batchChainStartHeight))

		err = batchProvider.BatchFetcher(ctx, rs.batchCh, batchChainStartHeight, &lastL2Height)
		if err != nil {
			rs.logger.Error("batch provider", "fetcher", err.Error())
			return err
		}

		batchChainStartHeight = 1
		batchInfoIndex++
	}
}

func (rs *RollupSyncer) batchProcessor(ctx context.Context, targetL2Height uint64) error {
	var batchDataHeader *rstypes.BatchDataHeader
	var chunks map[uint64][]byte
	chunksLength := 0

	// endChecker := time.NewTicker(100 * time.Millisecond)
	// defer endChecker.Stop()

	for {
		select {
		// case <-endChecker.C:
		// 	select {
		// 	case <-done:
		// 		if len(rs.batchCh) == 0 {
		// 			return errors.New("batch provider early closed")
		// 		}
		// 	default:
		// 	}

		case <-ctx.Done():
			return ctx.Err()
		case batchInfo := <-rs.batchCh:
			rs.logger.Debug("received a batch chunk", "height", batchInfo.BatchChainHeight, "tx_index", batchInfo.TxIndex)

			// if batch header is not initialized, always look for the header
			// first.

			switch rstypes.BatchDataType(batchInfo.Batch[0]) {
			case rstypes.BatchDataTypeHeader:
				dataHeader, err := rstypes.UnmarshalBatchDataHeader(batchInfo.Batch)
				if err != nil {
					return err
				}
				batchDataHeader = &dataHeader
				rs.logger.Debug("received a batch header", "start", batchDataHeader.Start, "end", batchDataHeader.End, "chunks", len(batchDataHeader.Checksums))
				chunks = make(map[uint64][]byte)

			case rstypes.BatchDataTypeChunk:
				if batchDataHeader.Checksums == nil || chunks == nil {
					// if the header is not initialized, skip the chunk
					continue
				}

				dataChunk, err := rstypes.UnmarshalBatchDataChunk(batchInfo.Batch)
				if err != nil {
					rs.logger.Error("failed to unmarshal batch data chunk", "error", err.Error())
					// ignore invalid chunk
					continue
				}
				checksum := sha256.Sum256(dataChunk.ChunkData)
				if uint64(len(batchDataHeader.Checksums)) <= dataChunk.Index {
					// ignore invalid chunk
					rs.logger.Error("invalid chunk index", "checksums", len(batchDataHeader.Checksums), "index", dataChunk.Index)
				} else if !bytes.Equal(checksum[:], batchDataHeader.Checksums[dataChunk.Index]) {
					// ignore invalid chunk
					rs.logger.Error("invalid chunk checksum", "header", batchDataHeader.Checksums[dataChunk.Index], "chunk", checksum)
				} else {
					chunks[dataChunk.Index] = dataChunk.ChunkData
					chunksLength += len(dataChunk.ChunkData)

					// if all chunks are received, reconstruct the batch data
					if len(chunks) == len(batchDataHeader.Checksums) {
						err := rs.handleCompleteChunks(ctx, batchDataHeader, chunks, chunksLength, batchInfo.BatchChainHeight)
						if err != nil {
							return err
						}
					}
					batchDataHeader = nil
					chunks = nil
				}
			}
		}
	}

	rs.logger.Info("Completed fetching batches")
	return nil
}

func (rs *RollupSyncer) handleCompleteChunks(ctx context.Context, batchDataHeader *rstypes.BatchDataHeader, chunks map[uint64][]byte, chunksLength int, batchChainHeight int64) error {
	batchBytes := make([]byte, 0, chunksLength)
	for _, chunk := range chunks {
		batchBytes = append(batchBytes, chunk...)
	}

	rawData, err := decompressBatch(batchBytes)
	if err != nil {
		return errors.Join(errors.New("failed to decompress batch"), err)
	}

	dataLength := len(rawData)
	rawBlocks := rawData[:dataLength-1]
	rawCommit := rawData[dataLength-1]

	for i, blockBytes := range rawBlocks {
		block, err := unmarshalBlock(blockBytes)
		if err != nil {
			rs.logger.Error("failed to unmarshal block", "raw_blocks_index", i, "error", err.Error())
			// ignore invalid block
			continue
		}

		err = rs.fillOracleData(ctx, block)
		if err != nil {
			return errors.Join(errors.New("failed to fill oracle data to block"), err)
		}

		err = block.ValidateBasic()
		if err != nil {
			return errors.Join(fmt.Errorf("invalid block: %d", block.Height), err)
		}

		rs.blockCh <- rstypes.BlockChanInfo{
			Block:            block,
			BatchChainHeight: batchChainHeight,
		}
	}

	commit, err := unmarshalCommit(rawCommit)
	if err != nil {
		rs.logger.Error("failed to unmarshal commit", "error", err.Error())
	} else {
		rs.blockCh <- rstypes.BlockChanInfo{
			Commit:           commit,
			BatchChainHeight: batchChainHeight,
		}
	}
	return nil
}
