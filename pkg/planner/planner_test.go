package planner_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
	"github.com/unhandled-exception/sophiadb/pkg/planner"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
	"github.com/unhandled-exception/sophiadb/pkg/tx/transaction"
)

type PlannerTestSuite struct {
	Suite
}

func TestPlannerTestSuite(t *testing.T) {
	suite.Run(t, new(PlannerTestSuite))
}

func (ts *PlannerTestSuite) newSUT() (planner.Planner, *transaction.Transaction, *metadata.Manager, func()) {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, t.TempDir())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	mdm, err := metadata.NewManager(true, trx)
	require.NoError(t, err)

	sut := planner.NewSQLPlanner(
		planner.NewSQLQueryPlanner(mdm),
		planner.NewSQLCommandsPlanner(mdm),
	)

	return sut, trx, mdm, func() {
		require.NoError(t, fm.Close())
	}
}

func (ts *PlannerTestSuite) table1Layout() records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")
	schema.AddInt64Field("_hidden")

	return records.NewLayout(schema)
}

func (ts *PlannerTestSuite) table2Layout() records.Layout {
	schema := records.NewSchema()
	schema.AddField("user_id", records.Int64Field, 0)
	schema.AddField("job", records.StringField, 20)
	schema.AddField("room", records.Int8Field, 0)
	schema.AddField("_invisible", records.Int64Field, 0)

	return records.NewLayout(schema)
}

func (ts *PlannerTestSuite) TestCreateQueryPlan_Ok() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.table1Layout().Schema, trx))
	require.NoError(t, mdm.CreateTable("table2", ts.table2Layout().Schema, trx))
	require.NoError(t, mdm.CreateView("view1", "select user_id, job from table2", trx))

	query := `
         SELECT id, name, job, age
       	  FROM table1, view1
         WHERE id=user_id
               and age = 25
    `

	plan, err := sut.CreateQueryPlan(query, trx)
	require.NoError(t, err)
	require.Equal(t,
		"choose id, name, job, age from (select from (join (scan table table1) to (choose user_id, job from (select from (scan table table2) where true))) where id = user_id and age = 25)",
		plan.String(),
	)
}

func (ts *PlannerTestSuite) TestCreateQueryPlan_Fail() {
	t := ts.T()

	sut, trx, _, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	var err error

	_, err = sut.CreateQueryPlan("select from table1", trx)
	assert.ErrorIs(t, err, parse.ErrBadSyntax)

	_, err = sut.CreateQueryPlan("insert into table1 (id) values (1)", trx)
	assert.ErrorIs(t, err, parse.ErrBadSyntax)

	_, err = sut.CreateQueryPlan("select id from table1", trx)
	assert.ErrorIs(t, err, planner.ErrFailedToCreatePlan)
}

func (ts *PlannerTestSuite) TestExecuteCommand_Fail() {
	t := ts.T()

	sut, trx, _, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	_, err := sut.ExecuteCommand("select from table1", trx)
	assert.ErrorIs(t, err, parse.ErrBadSyntax)

	_, err = sut.ExecuteCommand("select id from table1", trx)
	assert.ErrorIs(t, err, parse.ErrInvalidStatement)
}

func (ts *PlannerTestSuite) TestExecuteCreateTable() {
	t := ts.T()

	sut, trx, _, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	rows, err := sut.ExecuteCommand("create table table1 (id int64)", trx)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows)
}

func (ts *PlannerTestSuite) TestExecuteCreateIndex() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	rows, err := sut.ExecuteCommand("create index idx1 on table1 (id)", trx)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows)
}

func (ts *PlannerTestSuite) TestExecuteCreateView() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	rows, err := sut.ExecuteCommand("create view view1 as select id from table1", trx)
	assert.NoError(t, err)
	assert.Equal(t, 0, rows)
}

func (ts *PlannerTestSuite) TestExecuteDelete() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	sc, err := scan.NewTableScan(trx, "table1", ts.testLayout())
	require.NoError(t, err)

	defer sc.Close()

	cnt := 100
	for i := 0; i < cnt; i++ {
		require.NoError(t, sc.Insert())
		require.NoError(t, sc.SetInt64("id", int64(i)))
		require.NoError(t, sc.SetInt8("age", int8(i%10)))
	}

	ts.requireRowsCount(cnt, sc)

	rows, err := sut.ExecuteCommand("delete from table1 where age = 5", trx)
	require.NoError(t, err)

	assert.EqualValues(t, cnt/10, rows)
	ts.requireRowsCount(cnt-cnt/10, sc)
}

func (ts *PlannerTestSuite) TestExecuteUpdate() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	sc, err := scan.NewTableScan(trx, "table1", ts.testLayout())
	require.NoError(t, err)

	defer sc.Close()

	cnt := 100
	for i := 0; i < cnt; i++ {
		require.NoError(t, sc.Insert())
		require.NoError(t, sc.SetInt64("id", int64(i)))
		require.NoError(t, sc.SetInt8("age", int8(i%10)))
	}

	ts.requireRowsCount(cnt, sc)

	rows, err := sut.ExecuteCommand("update table1 set age = 99, name ='updated' where age = 5", trx)
	require.NoError(t, err)

	assert.EqualValues(t, cnt/10, rows)

	ts.requireRowsCount(10, scan.NewSelectScan(sc,
		scan.NewAndPredicate(
			scan.NewEqualTerm(
				scan.NewFieldExpression("age"),
				scan.NewScalarExpression(scan.NewInt8Constant(99)),
			),
			scan.NewEqualTerm(
				scan.NewFieldExpression("name"),
				scan.NewScalarExpression(scan.NewStringConstant("updated")),
			),
		),
	))
}

func (ts *PlannerTestSuite) TestExecuteInsert() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	sc, err := scan.NewTableScan(trx, "table1", ts.testLayout())
	require.NoError(t, err)

	defer sc.Close()

	ts.requireRowsCount(0, sc)

	cnt := 100

	for i := 0; i < cnt; i++ {
		rows, err := sut.ExecuteCommand(fmt.Sprintf("insert into table1 (id, name, age) values (%d, '%s', %d)", i, fmt.Sprintf("name %d", i), i%10), trx)
		require.NoError(t, err)
		require.EqualValues(t, 1, rows)
	}

	ts.requireRowsCount(cnt, sc)
}
