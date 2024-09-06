package types

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cometbft/cometbft/types"
	ophostv1 "github.com/initia-labs/OPinit/api/opinit/ophost/v1"
)

// prefix for chain type enum
const chainTypePrefix = "CHAIN_TYPE_"

// BatchChainTypeToString converts string batch chain type to BatchInfo_ChainType
func BatchChainTypeFromString(str string) ophostv1.BatchInfo_ChainType {
	return ophostv1.BatchInfo_ChainType(ophostv1.BatchInfo_ChainType_value[chainTypePrefix+str])
}

// BatchChainTypeToString converts BatchInfo_ChainType to string batch chain type
func BatchChainTypeToString(chainType ophostv1.BatchInfo_ChainType) string {
	return chainType.String()[len(chainTypePrefix):]
}

// BatchInfoUpdates is a list of BatchInfoUpdate
type BatchInfoUpdates []BatchInfoUpdate

// String returns a string representation of BatchInfoUpdates
func (b BatchInfoUpdates) String() string {
	res := ""
	for _, update := range b {
		res += fmt.Sprintf("| %d ~ %d: %s, %s ", update.Start, update.End, BatchChainTypeToString(update.ChainType), update.Submitter)
	}
	return res
}

// BatchInfoUpdate is a struct that contains information about a batch update
type BatchInfoUpdate struct {
	// Chain - need to fetch batch data from this chain
	ChainType ophostv1.BatchInfo_ChainType
	// Submitter - need to fetch batch data filtered by this submitter
	Submitter string
	// The starting height of this batch update applied
	Start int64
	// The ending height of this batch update applied
	End int64
}

// BatchChanInfo is a struct that contains information about a data required
// for batch data channel.
type BatchChanInfo struct {
	BatchInfoIndex int64
	// BatchChainHeight is the height that has already been checked to
	// search for the batch submitter's transaction.
	BatchChainHeight int64
	// TxIndex is the index of the transaction in the block
	TxIndex int64
	// Batch is the batch data
	Batch []byte
}

// BlockChanInfo is a struct that contains information about a data required
// for block data channel.
type BlockChanInfo struct {
	BatchInfoIndex int64
	// BatchChainHeight is the height that has already been checked to
	// search for the batch submitter's transaction.
	BatchChainHeight int64
	// Block is the block data
	Block *types.Block
	// Commit is the commit data
	Commit *types.Commit
}

type BatchDataType uint8

const (
	BatchDataTypeHeader BatchDataType = iota
	BatchDataTypeChunk
)

type BatchDataHeader struct {
	Start     uint64
	End       uint64
	Checksums [][]byte
}

type BatchDataChunk struct {
	Start     uint64
	End       uint64
	Index     uint64
	Length    uint64
	ChunkData []byte
}

func UnmarshalPartialHeader(data []byte) (BatchDataType, uint64, uint64, error) {
	if len(data) < 18 {
		err := errors.New("invalid data length")
		return 0, 0, 0, err
	}
	start, _ := binary.Uvarint(data[1:9])
	end, _ := binary.Uvarint(data[9:17])

	return BatchDataType(data[0]), start, end, nil
}

func MarshalBatchDataHeader(
	start uint64,
	end uint64,
	checksums [][]byte,
) []byte {
	data := make([]byte, 1)
	data[0] = byte(BatchDataTypeHeader)
	data = binary.AppendUvarint(data, start)
	data = binary.AppendUvarint(data, end)
	data = binary.AppendUvarint(data, uint64(len(checksums)))
	for _, checksum := range checksums {
		data = append(data, checksum...)
	}
	return data
}

func UnmarshalBatchDataHeader(data []byte) (BatchDataHeader, error) {
	if len(data) < 25 {
		err := errors.New("invalid data length")
		return BatchDataHeader{}, err
	}
	start, _ := binary.Uvarint(data[1:9])
	end, _ := binary.Uvarint(data[9:17])
	length, _ := binary.Uvarint(data[17:25])
	checksums := make([][]byte, 0, length)

	if len(data)-25%32 != 0 || (uint64(len(data)-25)/32) != length {
		err := errors.New("invalid checksum data")
		return BatchDataHeader{}, err
	}

	for i := 25; i < len(data); i += 32 {
		checksums = append(checksums, data[i:i+32])
	}

	return BatchDataHeader{
		Start:     start,
		End:       end,
		Checksums: checksums,
	}, nil
}

func MarshalBatchDataChunk(
	start uint64,
	end uint64,
	index uint64,
	length uint64,
	chunkData []byte,
) []byte {
	data := make([]byte, 1)
	data[0] = byte(BatchDataTypeChunk)
	data = binary.AppendUvarint(data, start)
	data = binary.AppendUvarint(data, end)
	data = binary.AppendUvarint(data, index)
	data = binary.AppendUvarint(data, length)
	data = append(data, chunkData...)
	return data
}

func UnmarshalBatchDataChunk(data []byte) (BatchDataChunk, error) {
	if len(data) < 33 {
		err := errors.New("invalid data length")
		return BatchDataChunk{}, err
	}
	start, _ := binary.Uvarint(data[1:9])
	end, _ := binary.Uvarint(data[9:17])
	index, _ := binary.Uvarint(data[17:25])
	length, _ := binary.Uvarint(data[25:33])
	chunkData := data[33:]

	return BatchDataChunk{
		Start:     start,
		End:       end,
		Index:     index,
		Length:    length,
		ChunkData: chunkData,
	}, nil
}
