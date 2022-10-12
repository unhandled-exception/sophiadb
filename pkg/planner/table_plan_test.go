package planner_test

import (
	"fmt"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/pkg/planner"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
	"github.com/unhandled-exception/sophiadb/pkg/tx/transaction"
)

var _ planner.Plan = &planner.TablePlan{}

type TablePlanTestSuite struct {
	Suite
}

func TestTablePlanSuite(t *testing.T) {
	suite.Run(t, new(TablePlanTestSuite))
}

func (ts *TablePlanTestSuite) newSUT(dataCount int) (*planner.TablePlan, *transaction.Transaction, func()) {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, t.TempDir())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	md, err := metadata.NewManager(true, trx)
	require.NoError(t, err)

	tablename := testDataTable

	require.NoError(t, md.CreateTable(tablename, ts.testLayout().Schema, trx))

	if dataCount > 0 {
		ts, werr := scan.NewTableScan(trx, tablename, ts.testLayout())
		require.NoError(t, werr)

		for i := 0; i < dataCount; i++ {
			require.NoErrorf(t, ts.Insert(), "write insert i == %d", i)

			require.NoErrorf(t, ts.SetInt64("id", int64(i+1)), "write int64 i == %d", i)
			require.NoErrorf(t, ts.SetInt8("age", int8(i%5)), "write int8 i == %d", i)
			require.NoErrorf(t, ts.SetString("name", fmt.Sprintf("user %d", i)), "write string i == %d", i)
		}
	}

	sut, err := planner.NewTablePlan(trx, tablename, md)
	require.NoError(t, err)

	return sut, trx, func() {
		fm.Close()
	}
}

func (ts *TablePlanTestSuite) TestPlan() {
	t := ts.T()

	const blocks = 1000
	cnt := int((defaultTestBlockSize / ts.testLayout().SlotSize) * blocks)

	sut, trx, clean := ts.newSUT(cnt)
	defer clean()

	assert.Equal(t, "scan table data", sut.String())
	assert.Equal(t, ts.testLayout().Schema.String(), sut.Schema().String())
	assert.EqualValues(t, cnt, sut.Records())
	assert.EqualValues(t, blocks, sut.BlocksAccessed())

	c, ok := sut.DistinctValues("age")
	assert.True(t, ok)
	assert.EqualValues(t, 5, c)

	sc, err := sut.Open()
	require.NoError(t, err)

	var i int64 = 0

	assert.NoError(t, scan.ForEach(sc, func() (bool, error) {
		i++

		v, err := sc.GetInt64("id")
		if err != nil {
			return true, err
		}

		assert.Equal(t, v, i)

		return false, nil
	}))

	require.NoError(t, trx.Commit())
}

func (ts *TablePlanTestSuite) TestUnexistantTable() {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, t.TempDir())
	defer fm.Close()

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	mc := minimock.NewController(t)
	md := planner.NewTablePlanMetadataManagerMock(mc).LayoutMock.Return(records.Layout{}, metadata.ErrTableNotFound)

	_, err = planner.NewTablePlan(trx, testDataTable, md)
	assert.ErrorIs(t, err, planner.ErrFailedToCreatePlan)

	require.NoError(t, trx.Commit())
}

func (ts *TablePlanTestSuite) TestMissedStats() {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, t.TempDir())
	defer fm.Close()

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	mc := minimock.NewController(t)
	md := planner.NewTablePlanMetadataManagerMock(mc).
		LayoutMock.Return(ts.testLayout(), nil).
		GetStatInfoMock.Return(metadata.StatInfo{}, metadata.ErrStatsMetadata)

	_, err = planner.NewTablePlan(trx, testDataTable, md)
	assert.ErrorIs(t, err, planner.ErrFailedToCreatePlan)

	require.NoError(t, trx.Commit())
}
