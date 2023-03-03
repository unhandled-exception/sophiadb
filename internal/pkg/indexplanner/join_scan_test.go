package indexplanner_test

import (
	"math"
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

var _ scan.Scan = &indexplanner.JoinScan{}

type JoinScanTestSuite struct {
	Suite
}

func TestJoinScanTestSute(t *testing.T) {
	suite.Run(t, new(JoinScanTestSuite))
}

func (ts *JoinScanTestSuite) TestJoinSelectScan_HashIndex() {
	t := ts.T()

	tm, sm := ts.newTRXManager(defaultLockTimeout, "")
	defer func() {
		require.NoError(t, sm.Close())
	}()

	table1Name := "table1"
	index1Name := "index1"
	t1Records := 100
	ts.makeTestTable(tm, table1Name, index1Name, indexes.HashIndexType, ts.testLayout(), t1Records)

	table2Name := "table2"
	index2Name := "index2"
	t2Records := 1000
	ts.makeTestTable(tm, table2Name, index2Name, indexes.HashIndexType, ts.testLayout2(), t2Records)

	ts.assertIndexedRecords(tm, table1Name, table2Name, index1Name, index2Name, indexes.HashIndexType, t1Records, t2Records, 8)
}

func (ts *JoinScanTestSuite) assertIndexedRecords(tm *transaction.TRXManager, table1Name string, table2Name string, index1Name string, index2Name string, idxType indexes.IndexType, t1recs int, t2recs int, testVal int8) {
	t := ts.T()

	tx, err := tm.Transaction()
	require.NoError(t, err)

	defer func() {
		require.NoError(t, tx.Commit())
	}()

	t1s, err := scan.NewTableScan(tx, table1Name, ts.testLayout())
	require.NoError(t, err)

	t2s, err := scan.NewTableScan(tx, table2Name, ts.testLayout2())
	require.NoError(t, err)

	idx, err := indexes.New(tx, idxType, index2Name, indexes.NewIndexLayout(records.Int8Field, 0))
	require.NoError(t, err)

	sut, err := indexplanner.NewJoinScan(t1s, idx, "age", t2s)
	require.NoError(t, err)

	defer sut.Close()

	assert.Equal(t, "id int64, name varchar(25), age int8, _hidden int64, id int64, age int8, vage int8", sut.Schema().String())
	assert.True(t, sut.HasField("age"))
	assert.False(t, sut.HasField("unknown"))

	cnt := 0

	require.NoError(t, scan.ForEach(sut, func() (stop bool, err error) {
		cnt++
		id, err := sut.GetInt64("id")
		require.NoError(t, err)

		_, err = sut.GetString("name")
		require.NoError(t, err)

		iAge, err := sut.GetInt8("age")
		require.NoError(t, err)
		assert.EqualValues(t, id%256, iAge)

		vAge, err := sut.GetVal("vage")
		require.NoError(t, err)

		assert.Equal(t, scan.CompEqual, vAge.CompareTo(scan.NewInt8Constant(iAge)))

		return false, nil
	}))

	assert.EqualValues(t, t1recs*int(math.Ceil(float64(t2recs)/256)), cnt)
}

func (ts *JoinScanTestSuite) testLayout2() records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddInt8Field("age")
	schema.AddInt8Field("vage")

	return records.NewLayout(schema)
}

func (ts *JoinScanTestSuite) makeTestTable(tm *transaction.TRXManager, tableName string, indexName string, idxType indexes.IndexType, layout records.Layout, recs int) {
	t := ts.T()

	tx, err := tm.Transaction()
	require.NoError(t, err)

	defer func() {
		require.NoError(t, tx.Commit())
	}()

	tts, err := scan.NewTableScan(tx, tableName, layout)
	require.NoError(t, err)

	defer tts.Close()

	idx, err := indexes.New(tx, idxType, indexName, indexes.NewIndexLayout(records.Int8Field, 0))
	require.NoError(t, err)

	for i := 0; i < recs; i++ {
		require.NoErrorf(t, tts.BeforeFirst(), "before first i == %d", i)
		require.NoErrorf(t, tts.Insert(), "write insert i == %d", i)

		require.NoErrorf(t, tts.SetInt64("id", int64(i)), "write int64 i == %d", i)
		require.NoErrorf(t, tts.SetInt8("age", int8(i%256)), "write int8 i == %d", i)

		if layout.Schema.HasField("vage") {
			require.NoErrorf(t, tts.SetInt8("vage", int8(i%256)), "write int8 i == %d", i)
		}

		require.NoError(t, idx.Insert(scan.NewInt8Constant(int8(i%256)), tts.RID()))
	}
}
