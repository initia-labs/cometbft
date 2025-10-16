package rollupsync

import (
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

//go:embed utils_test_data.json
var batchDataFixture []byte

func loadBatchDataHex(t *testing.T) string {
	t.Helper()

	var encoded string
	err := json.Unmarshal(batchDataFixture, &encoded)
	require.NoError(t, err)

	return encoded
}

func Test_IncompleteBatchData(t *testing.T) {
	rawHex := loadBatchDataHex(t)

	incompleteBatchData, err := hex.DecodeString(rawHex)
	require.NoError(t, err)

	blocks, err := decompressBatch(incompleteBatchData)
	require.NoError(t, err)
	require.Len(t, blocks, 62)
}
