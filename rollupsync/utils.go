package rollupsync

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"io"
	"math"
	"math/rand/v2"
	"time"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmtypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/gogoproto/proto"
	"github.com/pkg/errors"
)

func getLength(b []byte) int {
	return int(binary.LittleEndian.Uint64(b))
}

func decompressBatch(b []byte) (blocksBytes [][]byte, err error) {
	var res []byte
	for {
		br := bytes.NewReader(b)
		r, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}

		res, err = io.ReadAll(r)
		if err != nil {
			r.Close()

			if idx := bytes.Index(b[3:], []byte{0x1f, 0x8b, 0x08}); idx != -1 {
				recoveredBlocksBytes, err := recoverIncompleteBatch(b[:idx+3])
				if err != nil {
					return nil, err
				}
				blocksBytes = append(blocksBytes, recoveredBlocksBytes...)
				b = b[idx+3:]
				continue
			}

			return nil, err
		}

		defer r.Close()
		break
	}

	for offset := 0; offset < len(res); {
		bytesLength := getLength(res[offset : offset+8])
		offset += 8
		blocksBytes = append(blocksBytes, res[offset:offset+bytesLength])
		offset += bytesLength
	}
	return blocksBytes, nil
}

func recoverIncompleteBatch(batchData []byte) ([][]byte, error) {
	blocks := make([][]byte, 0)
	br := bytes.NewBuffer(batchData)
	reader, err := gzip.NewReader(br)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create gzip reader")
	}

	defer reader.Close()

	buf := new(bytes.Buffer)
	_, readErr := buf.ReadFrom(reader)

	data := buf.Bytes()

	partial := false
	for offset := 0; offset < len(data); {
		if len(data)-offset < 8 {
			partial = true
			break
		}
		length := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8

		if int(length) > len(data)-offset {
			partial = true
			break
		}

		block := make([]byte, int(length))
		copy(block, data[offset:offset+int(length)])
		blocks = append(blocks, block)
		offset += int(length)
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
		return nil, errors.Wrap(readErr, "failed to recover batch data")
	}
	if partial && errors.Is(readErr, io.ErrUnexpectedEOF) {
		return nil, errors.New("partial batch data detected")
	}
	return blocks, nil
}

// unmarshal block without validation.
//
// the validation will be performed after oracle data is fetched.
func unmarshalBlock(blockBz []byte) (*cmtypes.Block, error) {
	pbb := new(cmtproto.Block)
	err := proto.Unmarshal(blockBz, pbb)
	if err != nil {
		return nil, err
	}

	return cmtypes.BlockFromProtoWithNoValidation(pbb)
}

func unmarshalCommit(commitBz []byte) (*cmtypes.Commit, error) {
	pbc := new(cmtproto.Commit)
	err := proto.Unmarshal(commitBz, pbc)
	if err != nil {
		return nil, err
	}

	return cmtypes.CommitFromProto(pbc)
}

// SleepWithRetry repeatedly calls the worker function with exponential backoff until it succeeds.
// The backoff is calculated as: 2^retry * interval milliseconds with 50% jitter, capped at 5 seconds.
// Returns nil on success or context error on cancellation.
func SleepWithRetry(ctx context.Context, interval int64, worker func(retry int) bool) error {
	retry := 0
	for {
		if success := worker(retry); success {
			return nil
		}

		sleepTime := 2 * math.Exp2(float64(retry)) * float64(interval)
		sleepTime += rand.Float64() * sleepTime * 0.5
		sleepTime = math.Min(sleepTime, 5000) // max 5 seconds
		timer := time.NewTimer(time.Duration(sleepTime) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		timer.Stop()
		retry++
	}
}
