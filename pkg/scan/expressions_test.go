package scan_test

import (
	"math"
	"strconv"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

var (
	_ scan.Expression = scan.FieldExpression{}
	_ scan.Expression = scan.ScalarExpression{}
)

type ExpressionsTestSuite struct {
	Suite
}

func TestExpressionsTestSuite(t *testing.T) {
	suite.Run(t, new(ExpressionsTestSuite))
}

//nolint:forcetypeassert
func (ts *ExpressionsTestSuite) TestScalarExpression_Base() {
	t := ts.T()

	mc := minimock.NewController(t)
	plan := scan.NewPlanMock(mc)

	var value int64 = 1235

	sut := scan.NewScalarExpression(scan.NewInt64Constant(value))
	assert.False(t, sut.IsFieldName())
	assert.Equal(t, strconv.FormatInt(value, 10), sut.String())

	rf, ok := sut.ReductionFactor(plan)
	assert.EqualValues(t, math.MaxInt64, rf)
	assert.True(t, ok)

	val, vt := sut.Value()
	require.Equal(t, scan.ConstantValue, vt)
	assert.Equal(t, strconv.FormatInt(value, 10), val.(scan.Constant).String())

	layout := ts.testLayout()
	assert.True(t, sut.AppliesTo(layout.Schema))
}

func (ts *ExpressionsTestSuite) TestScalarExpression_Evaluate() {
	t := ts.T()

	var value int64 = 1235
	scalar := scan.NewInt64Constant(value)

	sut := scan.NewScalarExpression(scalar)
	layout := ts.testLayout()

	tm, sm := ts.newTRXManager(defaultLockTimeout, "")
	defer sm.Close()

	tx1, err := tm.Transaction()
	require.NoError(t, err)

	records := 10
	wts, err := scan.NewTableScan(tx1, testDataFile, layout)
	require.NoError(t, err)

	for i := 1; i < records+1; i++ {
		require.NoError(t, wts.Insert())
		require.NoError(t, wts.SetInt64("id", int64(i)))
	}

	require.NoError(t, tx1.Commit())

	tx2, err := tm.Transaction()
	require.NoError(t, err)

	rts, err := scan.NewTableScan(tx2, testDataFile, layout)
	require.NoError(t, err)

	assert.NoError(t, scan.ForEach(rts, func() (bool, error) {
		c, err := sut.Evaluate(rts)
		require.NoError(t, err)
		require.Equal(t, scan.CompEqual, c.CompareTo(scalar))

		return true, nil
	}))

	assert.NoError(t, tx2.Commit())
}

//nolint:forcetypeassert
func (ts *ExpressionsTestSuite) TestFieldExpression_Base() {
	t := ts.T()

	mc := minimock.NewController(t)
	plan := scan.NewPlanMock(mc)

	var idRF int64 = 12345

	plan.DistinctValuesMock.Expect("id").Return(idRF, true)

	fieldName := "id"
	sut := scan.NewFieldExpression(fieldName)

	assert.True(t, sut.IsFieldName())
	assert.Equal(t, fieldName, sut.String())

	rf, ok := sut.ReductionFactor(plan)
	assert.EqualValues(t, idRF, rf)
	assert.True(t, ok)

	val, vt := sut.Value()
	require.Equal(t, scan.StringValue, vt)
	assert.Equal(t, fieldName, val.(string))

	layout := ts.testLayout()
	assert.True(t, sut.AppliesTo(layout.Schema))

	emptySchema := records.NewSchema()
	assert.False(t, sut.AppliesTo(emptySchema))
}

func (ts *ExpressionsTestSuite) TestFieldExpression_Evaluate() {
	t := ts.T()

	fieldName := "id"
	sut := scan.NewFieldExpression(fieldName)

	layout := ts.testLayout()

	tm, sm := ts.newTRXManager(defaultLockTimeout, "")
	defer sm.Close()

	tx1, err := tm.Transaction()
	require.NoError(t, err)

	records := 10
	wts, err := scan.NewTableScan(tx1, testDataFile, layout)
	require.NoError(t, err)

	for i := 1; i < records+1; i++ {
		require.NoError(t, wts.Insert())
		require.NoError(t, wts.SetInt64("id", int64(i)))
	}

	require.NoError(t, tx1.Commit())

	tx2, err := tm.Transaction()
	require.NoError(t, err)

	rts, err := scan.NewTableScan(tx2, testDataFile, layout)
	require.NoError(t, err)

	i := 0

	assert.NoError(t, scan.ForEach(rts, func() (bool, error) {
		c, err := sut.Evaluate(rts)
		require.NoError(t, err)
		require.NotEqual(t, scan.CompEqual, c.CompareTo(scan.NewInt64Constant(int64(i))))

		i++

		return true, nil
	}))

	assert.NoError(t, tx2.Commit())
}
