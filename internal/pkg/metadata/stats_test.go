package metadata_test

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
)

const (
	testStatTable            = "stat_test"
	testStatTableRecords     = 12300
	testStatNameUniqueValues = 25
	testStatAgeUniqueValues  = 3
)

type StatsTestSuite struct {
	Suite
}

func TestStatsTestSuite(t *testing.T) {
	suite.Run(t, new(StatsTestSuite))
}

func (ts *StatsTestSuite) newSUT(t *testing.T, createTestTables bool) (*metadata.Stats, *metadata.Tables, *transaction.Transaction, func()) {
	trxMan, clean := ts.newTRXManager(defaultLockTimeout, t.TempDir())
	defer clean()

	strx, err := trxMan.Transaction()
	require.NoError(t, err)

	tables, err := metadata.NewTables(true, strx)
	require.NoError(t, err)

	if createTestTables {
		ts.createTestTable(t, tables, strx, testStatTableRecords)
	}

	sut, err := metadata.NewStats(tables, strx)
	require.NoError(t, err)

	require.NoError(t, strx.Commit())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	return sut, tables, trx, func() {
		clean()
	}
}

func (ts *StatsTestSuite) createTestTable(t *testing.T, tables *metadata.Tables, trx scan.TRXInt, recordsCount int) {
	schema := ts.newMethod()

	if recordsCount == 0 {
		recordsCount = testStatTableRecords
	}

	err := tables.CreateTable(testStatTable, schema, trx)

	switch {
	case errors.Is(err, metadata.ErrTableExists):
	default:
		require.NoError(t, err)
	}

	tab, err := scan.NewTableScan(trx, testStatTable, records.NewLayout(schema))
	require.NoError(t, err)

	defer tab.Close()

	for i := 0; i < recordsCount; i++ {
		require.NoError(t, tab.Insert())

		require.NoError(t, tab.SetInt64("id", int64(i)))
		require.NoError(t, tab.SetString("name", fmt.Sprintf("name (%d)", i%testStatNameUniqueValues)))
		require.NoError(t, tab.SetInt8("age", int8(i%testStatAgeUniqueValues)))
	}
}

func (*StatsTestSuite) newMethod() records.Schema {
	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	return schema
}

func (ts *StatsTestSuite) TestCreateStatsManagerAndRefreshStatistics() {
	t := ts.T()

	sut, tables, trx, clean := ts.newSUT(t, true)
	defer clean()

	layout := records.NewLayout(ts.newMethod())

	_, err := sut.GetStatInfo(tables.TcatTable, tables.TcatLayout, trx)
	assert.NoError(t, err)

	_, err = sut.GetStatInfo(tables.FcatTable, tables.FcatLayout, trx)
	assert.NoError(t, err)

	si, err := sut.GetStatInfo(testStatTable, layout, trx)
	require.NoError(t, err)

	assert.EqualValues(t, testStatTableRecords/(defaultTestBlockSize/layout.SlotSize)+1, si.Blocks)
	assert.EqualValues(t, testStatTableRecords, si.Records)

	idCnt, ok := si.DistinctValues("id")
	assert.True(t, ok)
	assert.InDelta(t, testStatTableRecords, idCnt, testStatTableRecords*0.05)

	nameCnt, ok := si.DistinctValues("name")
	assert.True(t, ok)
	assert.EqualValues(t, testStatNameUniqueValues, nameCnt)

	ageCnt, ok := si.DistinctValues("age")
	assert.True(t, ok)
	assert.EqualValues(t, testStatAgeUniqueValues, ageCnt)

	_, ok = si.DistinctValues("unexistant")
	assert.False(t, ok)
}

func (ts *StatsTestSuite) TestRefreshStatisticsOnNew() {
	t := ts.T()

	sut, tables, _, clean := ts.newSUT(t, true)
	defer clean()

	assert.True(t, sut.HasStatInfo(tables.TcatTable))
	assert.True(t, sut.HasStatInfo(tables.FcatTable))
	assert.True(t, sut.HasStatInfo(testStatTable))
}

func (ts *StatsTestSuite) TestRecalcStatisticsIfCallsCountsExpired() {
	t := ts.T()

	sut, tables, trx, clean := ts.newSUT(t, true)
	defer clean()

	assert.True(t, sut.HasStatInfo(testStatTable))

	layout := records.NewLayout(ts.newMethod())

	for i := 0; i < metadata.RefreshStatCalls-1; i++ {
		_, err := sut.GetStatInfo(testStatTable, layout, trx)
		require.NoError(t, err)
	}

	si1, err := sut.GetStatInfo(testStatTable, layout, trx)
	require.NoError(t, err)

	ts.createTestTable(t, tables, trx, 100)

	si2, err := sut.GetStatInfo(testStatTable, layout, trx)
	require.NoError(t, err)

	assert.Greater(t, si2.Records, si1.Records)
}

func (ts *StatsTestSuite) TestRecalcStatisticsIfTableStatsNotFound() {
	t := ts.T()

	sut, tables, trx, clean := ts.newSUT(t, false)
	defer clean()

	assert.False(t, sut.HasStatInfo(testStatTable))

	ts.createTestTable(t, tables, trx, 100)

	layout := records.NewLayout(ts.newMethod())

	si, err := sut.GetStatInfo(testStatTable, layout, trx)
	require.NoError(t, err)

	assert.EqualValues(t, 100, si.Records)
}

func (ts *StatsTestSuite) TestStatForUnexistantTable() {
	t := ts.T()

	sut, _, trx, clean := ts.newSUT(t, false)
	defer clean()

	layout := records.NewLayout(ts.newMethod())

	_, err := sut.GetStatInfo(testStatTable, layout, trx)
	assert.ErrorIs(t, err, metadata.ErrTableNotFound)
}
