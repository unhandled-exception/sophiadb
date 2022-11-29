package metadata_test

import (
	"sort"
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
	testManagerTableName = "test_table_1"
	testManagerViewName  = "test_view_1"
	testManagerViewDef   = "create view test_view_1 as select * from test_table_1"
	testManagerIndexName = "test_idx"
)

type MetadataManagerTestSute struct {
	Suite
}

func TestMetadataManagerTestSuite(t *testing.T) {
	suite.Run(t, new(MetadataManagerTestSute))
}

func (ts *MetadataManagerTestSute) newSUT() (*metadata.Manager, *transaction.Transaction, func()) {
	t := ts.T()

	path := ts.T().TempDir()

	trxMan, clean := ts.newTRXManager(defaultLockTimeout, path)

	strx, err := trxMan.Transaction()
	require.NoError(t, err)

	sut, err := metadata.NewManager(true, strx)
	require.NoError(t, err)

	require.NoError(t, strx.Commit())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	return sut, trx, func() {
		clean()
	}
}

func (ts *MetadataManagerTestSute) newTestTableLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	return records.NewLayout(schema)
}

func (ts *MetadataManagerTestSute) TestTablesMetadata() {
	t := ts.T()

	sut, trx, clean := ts.newSUT()
	defer clean()

	testLayout := ts.newTestTableLayout()

	require.NoError(t, sut.CreateTable(testManagerTableName, testLayout.Schema, trx))

	layout, err := sut.Layout(testManagerTableName, trx)
	require.NoError(t, err)

	assert.Equal(t, "schema: id int64, name varchar(25), age int8, slot size: 114", layout.String())

	tables := []string{}

	require.NoError(t, sut.ForEachTables(trx, func(tableName string) (stop bool, err error) {
		tables = append(tables, tableName)

		return false, nil
	}))

	sort.Strings(tables)
	assert.Equal(t, []string{
		"sbb_tables_fields",
		"sdb_indexes",
		"sdb_tables",
		"sdb_views",
		"test_table_1",
	}, tables)
}

func (ts *MetadataManagerTestSute) TestViewsMetadata() {
	t := ts.T()

	sut, trx, clean := ts.newSUT()
	defer clean()

	require.NoError(t, sut.CreateView(testManagerViewName, testManagerViewDef, trx))

	viewDef, err := sut.ViewDef(testManagerViewName, trx)
	require.NoError(t, err)

	assert.Equal(t, testManagerViewDef, viewDef)
}

func (ts *MetadataManagerTestSute) TestIndexesMetadata() {
	t := ts.T()

	sut, trx, clean := ts.newSUT()
	defer clean()

	testLayout := ts.newTestTableLayout()
	require.NoError(t, sut.CreateTable(testManagerTableName, testLayout.Schema, trx))

	require.NoError(t, sut.CreateIndex(testManagerIndexName, testManagerTableName, indexes.BTreeIndexType, "name", trx))

	indexes, err := sut.TableIndexes(testManagerTableName, trx)
	require.NoError(t, err)

	indexInfo, ok := indexes["name"]
	require.True(t, ok)
	assert.Equal(t, `"test_idx" on "test_table_1.name" using btree [blocks: 1, records 0, distinct values: 0]`, indexInfo.String())
}

func (ts *MetadataManagerTestSute) TestStatMetadata() {
	t := ts.T()

	sut, trx, clean := ts.newSUT()
	defer clean()

	testLayout := ts.newTestTableLayout()
	require.NoError(t, sut.CreateTable(testManagerTableName, testLayout.Schema, trx))

	statInfo, err := sut.GetStatInfo(testManagerTableName, testLayout, trx)
	require.NoError(t, err)

	assert.Equal(t, "blocks: 0, records: 0, distinct values: [age: 0, id: 0, name: 0]", statInfo.String())
}
