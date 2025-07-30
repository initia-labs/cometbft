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
)

func getLength(b []byte) int {
	return int(binary.LittleEndian.Uint64(b))
}

func decompressBatch(b []byte) ([][]byte, error) {
	br := bytes.NewReader(b)
	r, err := gzip.NewReader(br)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	res, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	blocksBytes := make([][]byte, 0)
	for offset := 0; offset < len(res); {
		bytesLength := getLength(res[offset : offset+8])
		offset += 8
		blocksBytes = append(blocksBytes, res[offset:offset+bytesLength])
		offset += bytesLength
	}
	return blocksBytes, nil
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
