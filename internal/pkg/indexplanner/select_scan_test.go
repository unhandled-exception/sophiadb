package indexplanner_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexplanner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
)

var _ scan.Scan = &indexplanner.SelectScan{}

type SelectScanTestSuite struct {
	Suite
}

func TestSelectScanTestSuite(t *testing.T) {
	suite.Run(t, new(SelectScanTestSuite))
}

func (ts *SelectScanTestSuite) TestIndexSelectScan_HashIndex() {
	t := ts.T()

	tm, sm := ts.newTRXManager(defaultLockTimeout, "")
	defer func() {
		require.NoError(t, sm.Close())
	}()

	tableName := "table1"
	indexName := "index1"
	records := 1000

	ts.makeTestTable(tm, tableName, indexName, indexes.HashIndexType, records)
	ts.assertIndexedRecords(tm, tableName, indexName, indexes.HashIndexType, records, 8)
}

func (ts *SelectScanTestSuite) makeTestTable(tm *transaction.TRXManager, tableName string, indexName string, idxType indexes.IndexType, recs int) {
	t := ts.T()

	tx, err := tm.Transaction()
	require.NoError(t, err)

	defer func() {
		require.NoError(t, tx.Commit())
	}()

	tts, err := scan.NewTableScan(tx, tableName, ts.testLayout())
	require.NoError(t, err)

	defer tts.Close()

	idx, err := indexes.New(tx, idxType, indexName, indexes.NewIndexLayout(records.Int8Field, 0))
	require.NoError(t, err)

	for i := 0; i < recs; i++ {
		require.NoErrorf(t, tts.BeforeFirst(), "before first i == %d", i)
		require.NoErrorf(t, tts.Insert(), "write insert i == %d", i)

		require.NoErrorf(t, tts.SetInt64("id", int64(i)), "write int64 i == %d", i)
		require.NoErrorf(t, tts.SetInt8("age", int8(i%256)), "write int8 i == %d", i)
		require.NoErrorf(t, tts.SetString("name", fmt.Sprintf("user %d", i)), "write string i == %d", i)

		require.NoError(t, idx.Insert(scan.NewInt8Constant(int8(i%256)), tts.RID()))
	}
}

func (ts *SelectScanTestSuite) assertIndexedRecords(tm *transaction.TRXManager, tableName string, indexName string, idxType indexes.IndexType, recs int, age int8) {
	t := ts.T()

	tx, err := tm.Transaction()
	require.NoError(t, err)

	defer func() {
		require.NoError(t, tx.Commit())
	}()

	tts, err := scan.NewTableScan(tx, tableName, ts.testLayout())
	require.NoError(t, err)

	idx, err := indexes.New(tx, idxType, indexName, indexes.NewIndexLayout(records.Int8Field, 0))
	require.NoError(t, err)

	sut, err := indexplanner.NewIndexSelectScan(tts, idx, scan.NewInt8Constant(age))
	require.NoError(t, err)

	defer sut.Close()

	assert.Equal(t, ts.testLayout().Schema, sut.Schema())

	assert.True(t, sut.HasField("age"))
	assert.False(t, sut.HasField("unknown"))

	cnt := 0

	require.NoError(t, scan.ForEach(sut, func() (stop bool, err error) {
		cnt++

		_, err = sut.GetInt64("id")
		require.NoError(t, err)

		_, err = sut.GetString("name")
		require.NoError(t, err)

		iAge, err := sut.GetInt8("age")
		require.NoError(t, err)
		assert.EqualValues(t, age, iAge)

		vAge, err := sut.GetVal("age")
		require.NoError(t, err)
		assert.Equal(t, scan.CompEqual, vAge.CompareTo(scan.NewInt8Constant(age)))

		return false, nil
	}))

	assert.LessOrEqual(t, cnt, recs/256+1)
	assert.GreaterOrEqual(t, cnt, recs/256)
}
