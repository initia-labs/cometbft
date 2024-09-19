package rollupsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cometbft/cometbft/rollupsync/provider"
	rstypes "github.com/cometbft/cometbft/rollupsync/types"

	comettypes "github.com/cometbft/cometbft/types"
)

func (rs *RollupSyncer) batchFetcher(ctx context.Context) error {
	height := rs.state.LastBlockHeight + 1

	batchInfos, err := rs.l1Provider.GetBatchInfos(ctx)
	if err != nil {
		return err
	}

	if len(batchInfos) == 0 {
		return errors.New("no batch info found")
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
			case <-ctx.Done():
				return
			case <-batchInfoUpdateTicker.C:
			}
			nextInfo, err := rs.l1Provider.GetNextBatchInfo(ctx, uint64(index))
			if err != nil {
				continue
			}
			if nextInfo != nil {
				batchInfos = append(batchInfos, nextInfo)
				*lastL2Height = nextInfo.Output.L2BlockNumber
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if batchInfoIndex >= len(batchInfos) {
			break
		}
		batchInfo := batchInfos[batchInfoIndex]
		lastL2Height := uint64(0)

		if batchInfoIndex < len(batchInfos)-1 {
			lastL2Height = batchInfos[batchInfoIndex+1].Output.L2BlockNumber
		} else if batchInfoIndex == len(batchInfos)-1 {
			lastL2Height = rs.targetBlockHeight
			go batchInfoUpdater(batchInfoIndex, &lastL2Height)
		}

		chainType := batchInfo.BatchInfo.ChainType
		batchProvider, err := provider.NewBatchProvider(rs.logger.With("provider", rstypes.BatchChainTypeToString(chainType)), rs.cfg, chainType, batchInfo.BatchInfo.Submitter, int64(batchInfoIndex))
		if err != nil {
			return err
		}

		rs.logger.Info(
			"batch info",
			"start_l2_block_number", batchInfo.Output.L2BlockNumber+1,
			"chain", batchInfo.BatchInfo.ChainType,
			"submitter", batchInfo.BatchInfo.Submitter,
			"index", batchInfoIndex,
		)

		fetchCtx, done := context.WithCancel(ctx)
		go batchProvider.BatchFetcher(fetchCtx, rs.batchCh, rs.batchChClosed, batchChainStartHeight)

		endChecker := time.NewTicker(time.Duration(rs.cfg.FetchInterval) * time.Millisecond)
	CTXLOOP:
		for {
			select {
			case <-fetchCtx.Done():
				break CTXLOOP
			case <-endChecker.C:
				if lastL2Height != 0 && uint64(rs.state.LastBlockHeight) >= lastL2Height {
					done()
				}
			}
		}
		endChecker.Stop()

		batchChainStartHeight = 1
		batchInfoIndex++
	}
	return nil
}

func (rs *RollupSyncer) batchProcessor(ctx context.Context) error {
	var batchDataHeader *rstypes.BatchDataHeader
	var chunks map[uint64][]byte
	chunkSize := 0

	defer close(rs.batchChClosed)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batchInfo := <-rs.batchCh:
			switch rstypes.BatchDataType(batchInfo.Batch[0]) {
			case rstypes.BatchDataTypeHeader:
				dataHeader, err := rstypes.UnmarshalBatchDataHeader(batchInfo.Batch)
				if err != nil {
					rs.logger.Info("failed to unmarshal batch data header", "error", err.Error())
					// ignore invalid header
					continue
				}
				batchDataHeader = &dataHeader
				rs.logger.Info(
					"received a batch header",
					"batch_chain_height", batchInfo.BatchChainHeight,
					"range", fmt.Sprintf("%d ~ %d", batchDataHeader.Start, batchDataHeader.End),
					"chunks", len(batchDataHeader.Checksums),
				)
				chunks = make(map[uint64][]byte)
				chunkSize = 0

			case rstypes.BatchDataTypeChunk:
				if batchDataHeader == nil || batchDataHeader.Checksums == nil || chunks == nil {
					// if the header is not initialized, skip the chunk
					continue
				}

				dataWithHeader, err := rstypes.UnmarshalBatchDataChunk(batchInfo.Batch)
				if err != nil {
					rs.logger.Debug("failed to unmarshal batch data chunk", "error", err.Error())
					// ignore invalid chunk
					continue
				}
				rs.logger.Info("received a batch chunk",
					"batch_chain_height", batchInfo.BatchChainHeight,
					"range", fmt.Sprintf("%d ~ %d", batchDataHeader.Start, batchDataHeader.End),
					"index", fmt.Sprintf("%d/%d", dataWithHeader.Index, dataWithHeader.Length),
					"chunk_size", len(dataWithHeader.ChunkData),
				)

				chunk := dataWithHeader.ChunkData
				checksum := rstypes.GetChecksumFromChunk(chunk)
				if uint64(len(batchDataHeader.Checksums)) <= dataWithHeader.Index {
					// ignore invalid chunk
					rs.logger.Info("invalid chunk index", "checksums", len(batchDataHeader.Checksums), "index", dataWithHeader.Index)
				} else if !bytes.Equal(checksum[:], batchDataHeader.Checksums[dataWithHeader.Index]) {
					// ignore invalid chunk
					rs.logger.Info("invalid chunk checksum", "header", batchDataHeader.Checksums[dataWithHeader.Index], "chunk", checksum)
				} else {
					chunks[dataWithHeader.Index] = chunk
					chunkSize += len(chunk)

					// if all chunks are received, reconstruct the batch data
					if len(chunks) == len(batchDataHeader.Checksums) {
						err := rs.handleCompleteChunks(ctx, len(batchDataHeader.Checksums), chunks, chunkSize, batchInfo.BatchChainHeight, batchInfo.BatchInfoIndex)
						if err != nil {
							return err
						}
						batchDataHeader = nil
						chunks = nil
					}
				}
			}
		}
	}
}

func (rs *RollupSyncer) handleCompleteChunks(ctx context.Context, chunkLength int, chunks map[uint64][]byte, chunkSize int, batchChainHeight int64, batchInfoIndex int64) error {
	rs.logger.Info("handle complete chunks", "chunks", chunkLength, "chunk_size", chunkSize)
	batchBytes := make([]byte, 0, chunkSize)
	for index := range chunks {
		chunk, ok := chunks[index]
		if !ok {
			rs.logger.Info("missing chunks", "index", index, "length", chunkLength)
			return nil
		}
		batchBytes = append(batchBytes, chunk...)
	}

	rawData, err := decompressBatch(batchBytes)
	if err != nil {
		rs.logger.Info("failed to decompress batch", "error", err.Error())
		return nil
	}

	dataLength := len(rawData)
	rawBlocks := rawData[:dataLength-1]
	rawCommit := rawData[dataLength-1]

	var lastBlock *comettypes.Block
	for i, blockBytes := range rawBlocks {
		block, err := unmarshalBlock(blockBytes)
		if err != nil {
			rs.logger.Info("failed to unmarshal block", "index", i, "length", len(rawBlocks), "error", err.Error())
			// ignore invalid block
			continue
		}
		lastBlock = block

		err = rs.fillOracleData(ctx, block)
		if err != nil {
			return errors.Join(errors.New("failed to fill oracle data to block"), err)
		}

		err = block.ValidateBasic()
		if err != nil {
			return errors.Join(fmt.Errorf("invalid block: %d", block.Height), err)
		}

		select {
		case <-rs.blockChClosed:
		case rs.blockCh <- rstypes.BlockChanInfo{
			Block:            block,
			BatchChainHeight: batchChainHeight,
			BatchInfoIndex:   batchInfoIndex,
		}:
		}
	}

	commit, err := unmarshalCommit(rawCommit)
	if err != nil {
		rs.logger.Info("failed to unmarshal commit", "error", err.Error())
	} else if lastBlock != nil && lastBlock.Height != commit.Height {
		rs.logger.Info("invalid commit height", "error", fmt.Sprintf("last block height: %d, commit height: %d", lastBlock.Height, commit.Height))
	} else {
		select {
		case <-rs.blockChClosed:
		case rs.blockCh <- rstypes.BlockChanInfo{
			Commit:           commit,
			BatchChainHeight: batchChainHeight,
			BatchInfoIndex:   batchInfoIndex,
		}:
		}
	}
	return nil
}
