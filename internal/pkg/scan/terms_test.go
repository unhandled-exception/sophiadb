package scan_test

import (
	"math"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

var _ scan.Term = scan.EqualTerm{}

type TermsTestSuite struct {
	Suite
}

func TestTermsTestSuite(t *testing.T) {
	suite.Run(t, new(TermsTestSuite))
}

func (ts *TermsTestSuite) TestEqualTerm_Base() {
	t := ts.T()

	layout := ts.testLayout()

	e1 := scan.NewFieldExpression("age")
	e2 := scan.NewScalarExpression(scan.NewInt8Constant(12))

	sut := scan.NewEqualTerm(e1, e2)

	assert.Equal(t, "age = 12", sut.String())
	assert.True(t, sut.AppliesTo(layout.Schema))
}

func (ts *TermsTestSuite) TestEqualTerm_isSatisfied() {
	t := ts.T()

	layout := ts.testLayout()

	e1 := scan.NewFieldExpression("id")

	records := 100

	var value int64 = 45
	e2 := scan.NewScalarExpression(scan.NewInt64Constant(value))

	sut := scan.NewEqualTerm(e1, e2)

	tm, sm := ts.newTRXManager(defaultLockTimeout, "")
	defer sm.Close()

	tx1, err := tm.Transaction()
	require.NoError(t, err)

	wts, err := scan.NewTableScan(tx1, testDataTable, layout)
	require.NoError(t, err)

	defer wts.Close()

	for i := 1; i < records+1; i++ {
		require.NoError(t, wts.Insert())
		require.NoError(t, wts.SetInt64("id", int64(i)))
	}

	require.NoError(t, tx1.Commit())

	tx2, err := tm.Transaction()
	require.NoError(t, err)

	rts, err := scan.NewTableScan(tx2, testDataTable, layout)
	require.NoError(t, err)

	defer rts.Close()

	var i int64 = 0

	assert.NoError(t, scan.ForEach(rts, func() (bool, error) {
		ok, err := sut.IsSatisfied(rts)
		require.NoError(t, err)

		if i+1 == value {
			assert.True(t, ok)
		} else {
			assert.False(t, ok)
		}

		i++

		return false, nil
	}))

	assert.EqualValues(t, records, i)

	assert.NoError(t, tx2.Commit())
}

func (ts *TermsTestSuite) TestEqualTerm_ReductionFactor() {
	t := ts.T()

	mc := minimock.NewController(t)
	plan := scan.NewPlanMock(mc)
	plan.DistinctValuesMock.When("id").Then(1345, true)
	plan.DistinctValuesMock.When("age").Then(1937, true)

	ef1 := scan.NewFieldExpression("id")
	ef2 := scan.NewFieldExpression("age")
	es1 := scan.NewScalarExpression(scan.NewInt64Constant(456))
	es2 := scan.NewScalarExpression(scan.NewInt8Constant(120))

	sut1 := scan.NewEqualTerm(ef1, es1)
	rf, ok := sut1.ReductionFactor(plan)
	assert.True(t, ok)
	assert.EqualValues(t, 1345, rf)

	sut2 := scan.NewEqualTerm(es2, ef2)
	rf, ok = sut2.ReductionFactor(plan)
	assert.True(t, ok)
	assert.EqualValues(t, 1937, rf)

	sut3 := scan.NewEqualTerm(ef1, ef2)
	rf, ok = sut3.ReductionFactor(plan)
	assert.True(t, ok)
	assert.EqualValues(t, 1937, rf)

	sut4 := scan.NewEqualTerm(es1, es2)
	rf, ok = sut4.ReductionFactor(plan)
	assert.True(t, ok)
	assert.EqualValues(t, math.MaxInt64, rf)

	sut5 := scan.NewEqualTerm(es1, es1)
	rf, ok = sut5.ReductionFactor(plan)
	assert.True(t, ok)
	assert.EqualValues(t, 1, rf)
}

func (ts *TermsTestSuite) TestEqualTerm_EquatesWithConstant() {
	t := ts.T()

	value := scan.NewInt64Constant(456)
	ef := scan.NewFieldExpression("id")
	es := scan.NewScalarExpression(value)

	sut1 := scan.NewEqualTerm(ef, es)
	c, ok := sut1.EquatesWithConstant("id")
	assert.True(t, ok)
	assert.EqualValues(t, scan.CompEqual, c.CompareTo(value))

	c, ok = sut1.EquatesWithConstant("unexistant")
	assert.False(t, ok)
	assert.Nil(t, c)

	sut2 := scan.NewEqualTerm(es, ef)
	c, ok = sut2.EquatesWithConstant("id")
	assert.True(t, ok)
	assert.EqualValues(t, scan.CompEqual, c.CompareTo(value))

	c, ok = sut2.EquatesWithConstant("unexistant")
	assert.False(t, ok)
	assert.Nil(t, c)
}

func (ts *TermsTestSuite) TestEqualTerm_EquatesWithField() {
	t := ts.T()

	value := scan.NewInt64Constant(456)
	ef1 := scan.NewFieldExpression("id")
	ef2 := scan.NewFieldExpression("age")
	es := scan.NewScalarExpression(value)

	sut1 := scan.NewEqualTerm(ef1, es)
	fieldName, ok := sut1.EquatesWithField("id")
	assert.False(t, ok)
	assert.Equal(t, "", fieldName)

	sut2 := scan.NewEqualTerm(ef1, ef2)
	fieldName, ok = sut2.EquatesWithField("id")
	assert.True(t, ok)
	assert.Equal(t, "age", fieldName)

	fieldName, ok = sut2.EquatesWithField("age")
	assert.True(t, ok)
	assert.Equal(t, "id", fieldName)

	fieldName, ok = sut2.EquatesWithField("unexistant")
	assert.False(t, ok)
	assert.Equal(t, "", fieldName)
}
