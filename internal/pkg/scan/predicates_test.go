package scan_test

import (
	"fmt"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

var _ scan.Predicate = &scan.AndPredicate{}

type PredicatesTestsuite struct {
	Suite
}

func TestPredicatesTestsuite(t *testing.T) {
	suite.Run(t, new(PredicatesTestsuite))
}

func (ts *PredicatesTestsuite) secondTestSchema() records.Schema {
	schema := records.NewSchema()
	schema.AddField("login", records.StringField, 20)
	schema.AddField("first_name", records.StringField, 100)
	schema.AddField("last_name", records.StringField, 100)

	return schema
}

func (ts *PredicatesTestsuite) TestAndPredicate_EmptyPredicate() {
	t := ts.T()

	sut := scan.NewAndPredicate()

	assert.Len(t, sut.Terms(), 0)
	assert.Equal(t, "", sut.String())

	_, ok := sut.EquatesWithField("id")
	assert.False(t, ok)

	_, ok = sut.EquatesWithConstant("id")
	assert.False(t, ok)

	mc := minimock.NewController(t)
	plan := scan.NewPlanMock(mc)

	rf, ok := sut.ReductionFactor(plan)
	assert.True(t, ok)
	assert.EqualValues(t, 1, rf)

	layout := ts.testLayout()
	sp := sut.SelectSubPred(layout.Schema)
	require.NotNil(t, sp)
	assert.Len(t, sp.Terms(), 0)

	jsp := sut.JoinSubPred(layout.Schema, ts.secondTestSchema())
	require.Nil(t, jsp)

	sut.ConjoinWith(scan.NewAndPredicate(
		scan.NewEqualTerm(
			scan.NewFieldExpression("id"),
			scan.NewScalarExpression(scan.NewInt64Constant(13)),
		),
	))
	assert.Equal(t, "id = 13", sut.String())
}

func (ts *PredicatesTestsuite) TestAndPredicate_OneTerm() {
	t := ts.T()

	var idVal int64 = 12

	sut := scan.NewAndPredicate(scan.NewEqualTerm(
		scan.NewFieldExpression("id"),
		scan.NewScalarExpression(scan.NewInt64Constant(idVal)),
	))

	assert.Len(t, sut.Terms(), 1)
	assert.Equal(t, "id = 12", sut.String())

	_, ok := sut.EquatesWithField("id")
	assert.False(t, ok)

	c, ok := sut.EquatesWithConstant("id")
	assert.True(t, ok)
	assert.Equal(t, scan.CompEqual, c.CompareTo(scan.NewInt64Constant(idVal)))

	mc := minimock.NewController(t)
	plan := scan.NewPlanMock(mc)

	plan.DistinctValuesMock.When("id").Then(13, true)

	rf, ok := sut.ReductionFactor(plan)
	assert.True(t, ok)
	assert.EqualValues(t, 13, rf)

	layout := ts.testLayout()
	sp := sut.SelectSubPred(layout.Schema)
	require.NotNil(t, sp)
	assert.Len(t, sp.Terms(), len(sut.Terms()))

	jsp := sut.JoinSubPred(layout.Schema, ts.secondTestSchema())
	require.Nil(t, jsp)

	sut.ConjoinWith(scan.NewAndPredicate(
		scan.NewEqualTerm(
			scan.NewFieldExpression("id"),
			scan.NewScalarExpression(scan.NewInt64Constant(13)),
		),
	))
	assert.Equal(t, "id = 12 and id = 13", sut.String())
}

func (ts *PredicatesTestsuite) TestAndPredicate_MiltipleTerm() {
	t := ts.T()

	var idVal int64 = 12

	sut := scan.NewAndPredicate(
		scan.NewEqualTerm(
			scan.NewFieldExpression("id"),
			scan.NewScalarExpression(scan.NewInt64Constant(idVal)),
		),
		scan.NewEqualTerm(
			scan.NewFieldExpression("age"),
			scan.NewScalarExpression(scan.NewInt8Constant(8)),
		),
		scan.NewEqualTerm(
			scan.NewFieldExpression("name"),
			scan.NewScalarExpression(scan.NewStringConstant("Ivan")),
		),
		scan.NewEqualTerm(
			scan.NewFieldExpression("name"),
			scan.NewFieldExpression("login"),
		),
		scan.NewEqualTerm(
			scan.NewScalarExpression(scan.NewInt8Constant(15)),
			scan.NewScalarExpression(scan.NewInt8Constant(15)),
		),
	)

	assert.Len(t, sut.Terms(), 5)
	assert.Equal(t, "id = 12 and age = 8 and name = 'Ivan' and name = login and 15 = 15", sut.String())

	_, ok := sut.EquatesWithField("id")
	assert.False(t, ok)

	f, ok := sut.EquatesWithField("name")
	assert.True(t, ok)
	assert.Equal(t, "login", f)

	_, ok = sut.EquatesWithConstant("login")
	assert.False(t, ok)

	c, ok := sut.EquatesWithConstant("id")
	assert.True(t, ok)
	assert.Equal(t, scan.CompEqual, c.CompareTo(scan.NewInt64Constant(idVal)))

	mc := minimock.NewController(t)
	plan := scan.NewPlanMock(mc)

	plan.DistinctValuesMock.When("id").Then(13, true)
	plan.DistinctValuesMock.When("name").Then(17, true)
	plan.DistinctValuesMock.When("login").Then(21, true)
	plan.DistinctValuesMock.Return(0, false)

	rf, ok := sut.ReductionFactor(plan)
	assert.True(t, ok)
	assert.EqualValues(t, 13*17*21, rf)

	layout := ts.testLayout()
	sp := sut.SelectSubPred(layout.Schema)
	require.NotNil(t, sp)
	assert.Len(t, sp.Terms(), len(sut.Terms())-1)
	assert.Equal(t, "id = 12 and age = 8 and name = 'Ivan' and 15 = 15", sp.String())

	jsp := sut.JoinSubPred(layout.Schema, ts.secondTestSchema())
	require.NotNil(t, jsp)
	assert.Equal(t, "name = login", jsp.String())

	oldPredString := sut.String()
	sut.ConjoinWith(scan.NewAndPredicate(
		scan.NewEqualTerm(
			scan.NewFieldExpression("id"),
			scan.NewScalarExpression(scan.NewInt64Constant(13)),
		),
	))
	assert.Equal(t, oldPredString+" and id = 13", sut.String())
}

func (ts *PredicatesTestsuite) TestAndPredicate_IsSatisfied() {
	t := ts.T()

	tm, sm := ts.newTRXManager(defaultLockTimeout, "")
	defer sm.Close()

	tx1, err := tm.Transaction()
	require.NoError(t, err)

	wts, err := scan.NewTableScan(tx1, testDataTable, ts.testLayout())
	require.NoError(t, err)

	defer wts.Close()

	records := 1000

	for i := 1; i < records+1; i++ {
		require.NoError(t, wts.Insert())
		require.NoError(t, wts.SetInt64("id", int64(i)))
		require.NoError(t, wts.SetInt8("age", int8(i%256)))
		require.NoError(t, wts.SetString("name", fmt.Sprintf("user %d", i)))
	}

	require.NoError(t, tx1.Commit())

	tx2, err := tm.Transaction()
	require.NoError(t, err)

	rts, err := scan.NewTableScan(tx2, testDataTable, ts.testLayout())
	require.NoError(t, err)

	defer rts.Close()

	sut1 := scan.NewAndPredicate(
		scan.NewEqualTerm(
			scan.NewScalarExpression(scan.NewInt64Constant(3456)),
			scan.NewScalarExpression(scan.NewInt64Constant(6543)),
		),
	)

	sut2 := scan.NewAndPredicate(
		scan.NewEqualTerm(
			scan.NewFieldExpression("id"),
			scan.NewScalarExpression(scan.NewInt64Constant(323)),
		),
	)

	i := 0

	require.NoError(t,
		scan.ForEach(rts, func() (stop bool, err error) {
			ok, err := sut1.IsSatisfied(rts)
			require.NoError(t, err)
			assert.False(t, ok)

			id, err := rts.GetInt64("id")
			if err != nil {
				return true, err
			}

			if id == 323 {
				wok, werr := sut2.IsSatisfied(rts)
				if werr != nil {
					return true, werr
				}

				assert.True(t, wok)
			}

			i++

			return false, nil
		}),
	)

	require.Equal(t, records, i)

	assert.NoError(t, tx2.Commit())
}
