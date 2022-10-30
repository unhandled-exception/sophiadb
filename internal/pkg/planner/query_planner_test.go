package planner_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/internal/pkg/parse"
	"github.com/unhandled-exception/sophiadb/internal/pkg/planner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
)

type QueryPlannerTestSuite struct {
	Suite
}

func TestQueryPlannerTestSuite(t *testing.T) {
	suite.Run(t, new(QueryPlannerTestSuite))
}

func (ts *QueryPlannerTestSuite) newSUT() (planner.QueryPlanner, *transaction.Transaction, *metadata.Manager, func()) {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, t.TempDir())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	mdm, err := metadata.NewManager(true, trx)
	require.NoError(t, err)

	qp := planner.NewSQLQueryPlanner(mdm)

	return qp, trx, mdm, func() {
		require.NoError(t, fm.Close())
	}
}

func (ts *QueryPlannerTestSuite) table1Layout() records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")
	schema.AddInt64Field("_hidden")

	return records.NewLayout(schema)
}

func (ts *QueryPlannerTestSuite) table2Layout() records.Layout {
	schema := records.NewSchema()
	schema.AddField("user_id", records.Int64Field, 0)
	schema.AddField("job", records.StringField, 20)
	schema.AddField("room", records.Int8Field, 0)
	schema.AddField("_invisible", records.Int64Field, 0)

	return records.NewLayout(schema)
}

func (ts *QueryPlannerTestSuite) TestCreatePlan_Ok() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.table1Layout().Schema, trx))
	require.NoError(t, mdm.CreateTable("table2", ts.table2Layout().Schema, trx))
	require.NoError(t, mdm.CreateView("view1", "select user_id, job from table2", trx))

	_, stmt, err := parse.ParseQuery(`
         SELECT id, name, job, age
       	  FROM table1, view1
         WHERE id=user_id
               and age = 25
     `)
	require.NoError(t, err)

	plan, err := sut.CreatePlan(stmt.(parse.SelectStatement), trx)
	require.NoError(t, err)
	require.Equal(t,
		"choose id, name, job, age from (select from (join (scan table table1) to (choose user_id, job from (select from (scan table table2) where true))) where id = user_id and age = 25)",
		plan.String(),
	)
}

func (ts *QueryPlannerTestSuite) TestCreatePlan_Fail() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	require.NoError(t, trx.Commit())

	_, stmt, err := parse.ParseQuery(`
         SELECT id, name, job, age
       	  FROM table1, view1
         WHERE id=user_id
               and age = 25
     `)
	require.NoError(t, err)

	_, err = sut.CreatePlan(stmt.(parse.SelectStatement), trx)
	require.ErrorIs(t, err, planner.ErrFailedToCreatePlan)

	require.NoError(t, mdm.CreateTable("table1", ts.table1Layout().Schema, trx))
	require.NoError(t, mdm.CreateTable("table2", ts.table2Layout().Schema, trx))

	_, err = sut.CreatePlan(stmt.(parse.SelectStatement), trx)
	require.ErrorIs(t, err, planner.ErrFailedToCreatePlan)

	require.NoError(t, mdm.CreateView("view1", "select ... from table2", trx))

	_, err = sut.CreatePlan(stmt.(parse.SelectStatement), trx)
	require.ErrorIs(t, err, parse.ErrBadSyntax)
}
