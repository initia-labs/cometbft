package rollupsync

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/rollupsync/types"
	cmtypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/gogoproto/proto"
)

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

	encodedBlocks := bytes.Split(res, []byte(","))
	for _, encodedBlock := range encodedBlocks {
		blockBytes, err := base64.StdEncoding.DecodeString(string(encodedBlock))
		if err != nil {
			return nil, types.ErrBatchDecodingError
		}
		blocksBytes = append(blocksBytes, blockBytes)
	}
	return blocksBytes, nil
}

func unmarshalBlock(blockBz []byte) (*cmtypes.Block, error) {
	pbb := new(cmtproto.Block)
	err := proto.Unmarshal(blockBz, pbb)
	if err != nil {
		return nil, err
	}

	return cmtypes.BlockFromProto(pbb)
}

func unmarshalCommit(commitBz []byte) (*cmtypes.Commit, error) {
	pbc := new(cmtproto.Commit)
	err := proto.Unmarshal(commitBz, pbc)
	if err != nil {
		return nil, err
	}

	return cmtypes.CommitFromProto(pbc)
}
