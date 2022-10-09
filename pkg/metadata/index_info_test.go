package metadata_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/pkg/records"
)

const (
	testIndexInfoIndexName               = "test_index"
	testIndexInfoFieldName1              = "test_field_1"
	testIndexInfoFieldName2              = "test_field_2"
	testIndexInfoFieldName3              = "test_field_3"
	testIndexInfoRecords                 = 123456
	testIndexInfoBlocks                  = 456
	testIndexInfoIndex1DistinctValues    = 16
	testIndexInfoHashIndexBlocksAcessed  = 7
	testIndexInfoBTreeIndexBlocksAcessed = 2
)

type IndexInfoTestSuite struct {
	Suite
}

func TestIndexInfoTestSuite(t *testing.T) {
	suite.Run(t, new(IndexInfoTestSuite))
}

func (ts *IndexInfoTestSuite) newTestSchema() (records.Layout, metadata.StatInfo) {
	schema := records.NewSchema()
	schema.AddInt64Field(testIndexInfoFieldName1)
	schema.AddInt8Field(testIndexInfoFieldName2)
	schema.AddStringField(testIndexInfoFieldName3, 100)

	si := metadata.NewStatInfo(schema)
	si.Records = testIndexInfoRecords
	si.Blocks = testIndexInfoBlocks

	for i := 0; i < testIndexInfoIndex1DistinctValues; i++ {
		si.UpdateDistincValues(testIndexInfoFieldName1, []byte(fmt.Sprintf("value %d", i)))
	}

	return records.NewLayout(schema), si
}

func (ts *IndexInfoTestSuite) TestNewHashIndexInfo() {
	t := ts.T()

	trxMan, clean := ts.newTRXManager(defaultLockTimeout, "")
	defer clean()

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	layout, si := ts.newTestSchema()

	sut := metadata.NewIndexInfo(testIndexInfoIndexName, testIndexesTestTable1, indexes.HashIndexType, testIndexInfoFieldName1, layout.Schema, trx, si)

	assert.EqualValues(t, testIndexInfoRecords/testIndexInfoIndex1DistinctValues, sut.Records())
	assert.EqualValues(t, testIndexInfoHashIndexBlocksAcessed, sut.BlocksAccessed())
	assert.EqualValues(t, testIndexInfoIndex1DistinctValues, sut.DistinctValues(testIndexInfoFieldName1))

	assert.EqualValues(t, `"test_index" on "test_table_1.test_field_1" [hash blocks: 7, records 7716, distinct values: 16]`, sut.String())

	idx, err := sut.Open()
	require.NoError(t, err)

	assert.EqualValues(t, indexes.HashIndexType, idx.Type())
	assert.EqualValues(t, testIndexInfoIndexName, idx.Name())
	assert.EqualValues(t, "block int64, id int64, dataval int64", idx.Layout().Schema.String())
}

func (ts *IndexInfoTestSuite) TestNewBTreeIndexInfo() {
	t := ts.T()

	trxMan, clean := ts.newTRXManager(defaultLockTimeout, "")
	defer clean()

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	layout, si := ts.newTestSchema()

	sut := metadata.NewIndexInfo(testIndexInfoIndexName, testIndexesTestTable1, indexes.BTreeIndexType, testIndexInfoFieldName1, layout.Schema, trx, si)

	assert.EqualValues(t, testIndexInfoRecords/testIndexInfoIndex1DistinctValues, sut.Records())
	assert.EqualValues(t, testIndexInfoBTreeIndexBlocksAcessed, sut.BlocksAccessed())
	assert.EqualValues(t, testIndexInfoIndex1DistinctValues, sut.DistinctValues(testIndexInfoFieldName1))

	idx, err := sut.Open()
	require.NoError(t, err)

	assert.EqualValues(t, indexes.BTreeIndexType, idx.Type())
	assert.EqualValues(t, testIndexInfoIndexName, idx.Name())
	assert.EqualValues(t, "block int64, id int64, dataval int64", idx.Layout().Schema.String())
}

func (ts *IndexInfoTestSuite) TestCreateLayout() {
	t := ts.T()

	trxMan, clean := ts.newTRXManager(defaultLockTimeout, "")
	defer clean()

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	layout, si := ts.newTestSchema()

	sut1 := metadata.NewIndexInfo(testIndexInfoIndexName, testIndexesTestTable1, indexes.HashIndexType, testIndexInfoFieldName1, layout.Schema, trx, si)
	idx1, err := sut1.Open()
	require.NoError(t, err)
	assert.EqualValues(t, "block int64, id int64, dataval int64", idx1.Layout().Schema.String())

	sut2 := metadata.NewIndexInfo(testIndexInfoIndexName, testIndexesTestTable1, indexes.HashIndexType, testIndexInfoFieldName2, layout.Schema, trx, si)
	idx2, err := sut2.Open()
	require.NoError(t, err)
	assert.EqualValues(t, "block int64, id int64, dataval int8", idx2.Layout().Schema.String())

	sut3 := metadata.NewIndexInfo(testIndexInfoIndexName, testIndexesTestTable1, indexes.HashIndexType, testIndexInfoFieldName3, layout.Schema, trx, si)
	idx3, err := sut3.Open()
	require.NoError(t, err)
	assert.EqualValues(t, "block int64, id int64, dataval varchar(100)", idx3.Layout().Schema.String())
}
