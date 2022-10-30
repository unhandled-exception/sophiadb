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

var _ planner.Plan = &planner.SelectPlan{}

type SelectPlanTestSuite struct {
	Suite
}

func TestSelectPlanTestSuite(t *testing.T) {
	suite.Run(t, new(SelectPlanTestSuite))
}

func (ts *SelectPlanTestSuite) newSUT(pred scan.Predicate, dataCount int) (*planner.SelectPlan, *transaction.Transaction, func()) {
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

	sut, err := planner.NewSelectPlan(tp, pred)
	require.NoError(t, err)

	return sut, trx, func() {
		fm.Close()
	}
}

func (ts *SelectPlanTestSuite) TestPlan() {
	t := ts.T()

	var testID int64 = 777

	const blocks = 100
	cnt := int((defaultTestBlockSize / ts.testLayout().SlotSize) * blocks)

	pred := scan.NewAndPredicate(
		scan.NewEqualTerm(
			scan.NewFieldExpression("id"),
			scan.NewScalarExpression(scan.NewInt64Constant(testID)),
		),
	)

	sut, trx, clean := ts.newSUT(pred, cnt)
	defer clean()

	assert.Equal(t, "select from (scan table data) where id = 777", sut.String())
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

		assert.EqualValues(t, v, testID)

		return false, nil
	}))

	assert.EqualValues(t, 1, i)

	require.NoError(t, trx.Commit())
}

func (ts *SelectPlanTestSuite) TestDistinctValues_EquatesConst() {
	t := ts.T()

	mc := minimock.NewController(t)

	pred := planner.NewPredicateMock(mc).
		EquatesWithConstantMock.Return(nil, true)

	plan := planner.NewPlanMock(mc)

	sut, err := planner.NewSelectPlan(plan, pred)
	require.NoError(t, err)

	dv, ok := sut.DistinctValues("field")
	assert.EqualValues(t, 1, dv)
	assert.True(t, ok)
}

func (ts *SelectPlanTestSuite) TestDistinctValues_EquatesTwoFields() {
	t := ts.T()

	mc := minimock.NewController(t)

	pred := planner.NewPredicateMock(mc).
		EquatesWithConstantMock.Return(nil, false).
		EquatesWithFieldMock.Return("f2", true)

	plan := planner.NewPlanMock(mc).
		DistinctValuesMock.When("f1").Then(345, true).
		DistinctValuesMock.When("f2").Then(999, true)

	sut, err := planner.NewSelectPlan(plan, pred)
	require.NoError(t, err)

	dv, ok := sut.DistinctValues("f1")
	assert.EqualValues(t, 999, dv)
	assert.True(t, ok)
}

func (ts *SelectPlanTestSuite) TestFailedToOpenPlan() {
	t := ts.T()

	mc := minimock.NewController(t)
	pred := planner.NewPredicateMock(mc)
	plan := planner.NewPlanMock(mc).OpenMock.Return(nil, scan.ErrScan)

	sut, err := planner.NewSelectPlan(plan, pred)
	require.NoError(t, err)

	sc, err := sut.Open()
	assert.Nil(t, sc)
	assert.ErrorIs(t, err, scan.ErrScan)
}
