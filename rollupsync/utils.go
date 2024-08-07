package rollupsync

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"

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
