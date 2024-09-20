package types

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/cometbft/cometbft/types"
	ophostv1 "github.com/initia-labs/OPinit/api/opinit/ophost/v1"
)

// prefix for chain type enum
const chainTypePrefix = "CHAIN_TYPE_"

// BatchChainTypeToString converts string batch chain type to BatchInfo_ChainType
func BatchChainTypeFromString(str string) ophostv1.BatchInfo_ChainType {
	return ophostv1.BatchInfo_ChainType(ophostv1.BatchInfo_ChainType_value[chainTypePrefix+strings.ToUpper(str)])
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
		err := fmt.Errorf("invalid data length: %d, expected > 18", len(data))
		return 0, 0, 0, err
	}
	start := binary.BigEndian.Uint64(data[1:9])
	end := binary.BigEndian.Uint64(data[9:17])
	if start > end {
		return 0, 0, 0, fmt.Errorf("invalid start: %d, end: %d", start, end)
	}

	return BatchDataType(data[0]), start, end, nil
}

func GetChecksumFromChunk(chunk []byte) [32]byte {
	return sha256.Sum256(chunk)
}

func MarshalBatchDataHeader(
	start uint64,
	end uint64,
	checksums [][]byte,
) []byte {
	data := make([]byte, 1)
	data[0] = byte(BatchDataTypeHeader)
	data = binary.BigEndian.AppendUint64(data, start)
	data = binary.BigEndian.AppendUint64(data, end)
	data = binary.BigEndian.AppendUint64(data, uint64(len(checksums)))
	for _, checksum := range checksums {
		data = append(data, checksum...)
	}
	return data
}

func UnmarshalBatchDataHeader(data []byte) (BatchDataHeader, error) {
	if len(data) < 25 {
		err := fmt.Errorf("invalid data length: %d, expected > 25", len(data))
		return BatchDataHeader{}, err
	}
	start := binary.BigEndian.Uint64(data[1:9])
	end := binary.BigEndian.Uint64(data[9:17])
	if start > end {
		return BatchDataHeader{}, fmt.Errorf("invalid start: %d, end: %d", start, end)
	}

	length := binary.BigEndian.Uint64(data[17:25])
	if (len(data)-25)%32 != 0 || (uint64(len(data)-25)/32) != length {
		err := fmt.Errorf("invalid checksum length: %d, data length: %d", length, len(data)-25)
		return BatchDataHeader{}, err
	}

	checksums := make([][]byte, 0, length)
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
	data = binary.BigEndian.AppendUint64(data, start)
	data = binary.BigEndian.AppendUint64(data, end)
	data = binary.BigEndian.AppendUint64(data, index)
	data = binary.BigEndian.AppendUint64(data, length)
	data = append(data, chunkData...)
	return data
}

func UnmarshalBatchDataChunk(data []byte) (BatchDataChunk, error) {
	if len(data) < 33 {
		err := fmt.Errorf("invalid data length: %d, expected > 33", len(data))
		return BatchDataChunk{}, err
	}
	start := binary.BigEndian.Uint64(data[1:9])
	end := binary.BigEndian.Uint64(data[9:17])
	if start > end {
		return BatchDataChunk{}, fmt.Errorf("invalid start: %d, end: %d", start, end)
	}
	index := binary.BigEndian.Uint64(data[17:25])
	length := binary.BigEndian.Uint64(data[25:33])
	chunkData := data[33:]

	return BatchDataChunk{
		Start:     start,
		End:       end,
		Index:     index,
		Length:    length,
		ChunkData: chunkData,
	}, nil
}
