package metadata_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
)

const (
	testIndexesTestTable1 = "test_table_1"
	testIndexesTestTable2 = "test_table_2"
	testIndexesIndex1Name = "tt_idx_1"
	testIndexesIndex2Name = "tt_idx_2"
	testIndexesIndex3Name = "tt_idx_3"
)

type IndexesTestSuite struct {
	Suite
}

func TestIndexesTestSuite(t *testing.T) {
	suite.Run(t, new(IndexesTestSuite))
}

func (ts *IndexesTestSuite) newSut() (*metadata.Indexes, *transaction.Transaction, *metadata.Tables, func()) {
	t := ts.T()

	trxMan, clean := ts.newTRXManager(defaultLockTimeout, "")

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	tables, err := metadata.NewTables(true, trx)
	require.NoError(t, err)

	stats, err := metadata.NewStats(tables, trx)
	require.NoError(t, err)

	indexes, err := metadata.NewIndexes(tables, true, stats, trx)
	require.NoError(t, err)

	return indexes, trx, tables, func() {
		clean()
	}
}

func (ts *IndexesTestSuite) createNewTestTable(tables *metadata.Tables, trx records.TSTRXInt, recordsCount int) records.Layout {
	t := ts.T()

	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	layout := records.NewLayout(schema)

	require.NoError(t, tables.CreateTable(testIndexesTestTable1, schema, trx))

	tab1, err := records.NewTableScan(trx, testIndexesTestTable1, layout)
	require.NoError(t, err)

	defer tab1.Close()

	for i := 0; i < recordsCount; i++ {
		require.NoError(t, tab1.Insert())

		require.NoError(t, tab1.SetInt64("id", int64(i)))
		require.NoError(t, tab1.SetString("name", fmt.Sprintf("name (%d)", i%testStatNameUniqueValues)))
		require.NoError(t, tab1.SetInt8("age", int8(i%256)))
	}

	tab2, err := records.NewTableScan(trx, testIndexesTestTable2, layout)
	require.NoError(t, err)

	defer tab2.Close()

	for i := 0; i < recordsCount/2; i++ {
		require.NoError(t, tab2.Insert())

		require.NoError(t, tab2.SetInt64("id", int64(i)))
		require.NoError(t, tab2.SetString("name", fmt.Sprintf("name (%d)", i%testStatNameUniqueValues)))
		require.NoError(t, tab2.SetInt8("age", int8(i%256)))
	}

	return layout
}

func (ts *IndexesTestSuite) TestCreateIndex_Ok() {
	t := ts.T()

	sut, trx, tables, clean := ts.newSut()
	defer clean()

	ts.createNewTestTable(tables, trx, 12345)

	assert.NoError(t, sut.CreateIndex(testIndexesIndex1Name, testIndexesTestTable1, indexes.HashIndexType, "id", trx))
	assert.NoError(t, sut.CreateIndex(testIndexesIndex2Name, testIndexesTestTable1, indexes.BTreeIndexType, "name", trx))

	indexes, err := sut.TableIndexes(testIndexesTestTable1, trx)
	require.NoError(t, err)

	idxID, ok := indexes["id"]
	assert.True(t, ok)
	assert.EqualValues(t, "\"tt_idx_1\" on \"test_table_1.id\" [hash blocks: 0, records 1, distinct values: 12187]", idxID.String())

	idxName, ok := indexes["name"]
	assert.True(t, ok)
	assert.EqualValues(t, "\"tt_idx_2\" on \"test_table_1.name\" [btree blocks: 3, records 493, distinct values: 25]", idxName.String())
}

func (ts *IndexesTestSuite) TestCreateIndex_IndexExists() {
	t := ts.T()

	sut, trx, tables, clean := ts.newSut()
	defer clean()

	ts.createNewTestTable(tables, trx, 12345)

	require.NoError(t, sut.CreateIndex(testIndexesIndex1Name, testIndexesTestTable1, indexes.HashIndexType, "id", trx))
	require.NoError(t, sut.CreateIndex(testIndexesIndex2Name, testIndexesTestTable2, indexes.HashIndexType, "id", trx))

	assert.ErrorIs(t, sut.CreateIndex(testIndexesIndex1Name, testIndexesTestTable1, indexes.BTreeIndexType, "name", trx), metadata.ErrIndexExists)
}

func (ts *IndexesTestSuite) TestCreateIndex_FieldIndexeds() {
	t := ts.T()

	sut, trx, tables, clean := ts.newSut()
	defer clean()

	ts.createNewTestTable(tables, trx, 12345)

	require.NoError(t, sut.CreateIndex(testIndexesIndex1Name, testIndexesTestTable1, indexes.HashIndexType, "id", trx))
	require.NoError(t, sut.CreateIndex(testIndexesIndex2Name, testIndexesTestTable2, indexes.BTreeIndexType, "id", trx))

	assert.ErrorIs(t, sut.CreateIndex(testIndexesIndex3Name, testIndexesTestTable1, indexes.BTreeIndexType, "id", trx), metadata.ErrFieldIndexed)
}
