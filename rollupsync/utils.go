package rollupsync

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"

	"github.com/cometbft/cometbft/rollupsync/types"
)

func DecompressBatch(b []byte) ([][]byte, error) {
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
