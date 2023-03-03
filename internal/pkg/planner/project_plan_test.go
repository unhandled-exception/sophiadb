package planner_test

import (
	"fmt"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/internal/pkg/planner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
)

var _ planner.Plan = &planner.ProjectPlan{}

type ProjectPlanTestSuite struct {
	Suite
}

func TestProjectPlanTestSuite(t *testing.T) {
	suite.Run(t, new(ProjectPlanTestSuite))
}

func (ts *ProjectPlanTestSuite) newSUT(dataCount int, fields ...string) (*planner.ProjectPlan, *transaction.Transaction, func()) {
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

	tp, err := planner.NewTablePlan(trx, tablename, md)
	require.NoError(t, err)

	sut, err := planner.NewProjectPlan(tp, fields...)
	require.NoError(t, err)

	return sut, trx, func() {
		fm.Close()
	}
}

func (ts *ProjectPlanTestSuite) TestPlan() {
	t := ts.T()

	const blocks = 100
	cnt := int((defaultTestBlockSize / ts.testLayout().SlotSize) * blocks)

	sut, trx, clean := ts.newSUT(cnt, "id", "age")
	defer clean()

	assert.Equal(t, "choose id, age from (scan table data)", sut.String())
	assert.Equal(t, "id int64, age int8", sut.Schema().String())
	assert.EqualValues(t, cnt, sut.Records())
	assert.EqualValues(t, blocks, sut.BlocksAccessed())

	c, ok := sut.DistinctValues("age")
	assert.True(t, ok)
	assert.EqualValues(t, 5, c)

	sc, err := sut.Open()
	require.NoError(t, err)

	var i int64 = 0

	assert.NoError(t, scan.ForEach(sc, func() (stop bool, err error) {
		i++

		v, err := sc.GetInt64("id")
		if err != nil {
			return true, err
		}

		assert.Equal(t, v, i)

		_, err = sc.GetString("name")
		assert.ErrorIs(t, err, scan.ErrFieldNotFound)

		return false, nil
	}))

	assert.EqualValues(t, cnt, i)

	require.NoError(t, trx.Commit())
}

func (ts *ProjectPlanTestSuite) TestFailedToOpenPlan() {
	t := ts.T()

	mc := minimock.NewController(t)
	plan := planner.NewPlanMock(mc).
		OpenMock.Return(nil, scan.ErrScan).
		SchemaMock.Return(ts.testLayout().Schema)

	sut, err := planner.NewProjectPlan(plan, "id", "age")
	require.NoError(t, err)

	sc, err := sut.Open()
	assert.Nil(t, sc)
	assert.ErrorIs(t, err, scan.ErrScan)
}

func (ts *ProjectPlanTestSuite) TestUnexistantPlansFields() {
	t := ts.T()

	mc := minimock.NewController(t)
	plan := planner.NewPlanMock(mc).
		SchemaMock.Return(ts.testLayout().Schema)

	sut, err := planner.NewProjectPlan(plan, "id", "age", "_unexistant")
	require.NoError(t, err)

	assert.Equal(t, "id int64, age int8", sut.Schema().String())
}
