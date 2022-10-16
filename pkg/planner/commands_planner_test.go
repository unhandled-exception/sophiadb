package planner_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
	"github.com/unhandled-exception/sophiadb/pkg/planner"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
	"github.com/unhandled-exception/sophiadb/pkg/tx/transaction"
)

type CommandsPlannerTestSuite struct {
	Suite
}

func TestCommandsPlannerTestSuite(t *testing.T) {
	suite.Run(t, new(CommandsPlannerTestSuite))
}

func (ts *CommandsPlannerTestSuite) newSUT() (planner.CommandsPlanner, *transaction.Transaction, *metadata.Manager, func()) {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, t.TempDir())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	mdm, err := metadata.NewManager(true, trx)
	require.NoError(t, err)

	cp := planner.NewSQLCommandsPlanner(mdm)

	return cp, trx, mdm, func() {
		require.NoError(t, fm.Close())
	}
}

func (ts *CommandsPlannerTestSuite) TestExecuteCreateTable_Ok() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	_, stmt, err := parse.ParseQuery(fmt.Sprintf("create table table1 (%s)", ts.testLayout().Schema))
	require.NoError(t, err)

	rows, err := sut.ExecuteCreateTable(
		stmt.(parse.CreateTableStatement),
		trx,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 0, rows)

	layout, err := mdm.Layout("table1", trx)
	require.NoError(t, err)

	assert.Equal(t, ts.testLayout().Schema.String(), layout.Schema.String())
}

func (ts *CommandsPlannerTestSuite) TestExecuteCreateTable_Fail() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery(fmt.Sprintf("create table table1 (%s)", ts.testLayout().Schema))
	require.NoError(t, err)

	_, err = sut.ExecuteCreateTable(
		stmt.(parse.CreateTableStatement),
		trx,
	)
	require.ErrorIs(t, err, planner.ErrExecuteError)
}

func (ts *CommandsPlannerTestSuite) TestExecuteCreateIndex_Ok() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("create index idx1 on table1(id)")
	require.NoError(t, err)

	rows, err := sut.ExecuteCreateIndex(
		stmt.(parse.CreateIndexStatement),
		trx,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 0, rows)

	_, stmt, err = parse.ParseQuery("create index idx2 on table1(name) using btree")
	require.NoError(t, err)

	rows, err = sut.ExecuteCreateIndex(
		stmt.(parse.CreateIndexStatement),
		trx,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 0, rows)

	indexes, err := mdm.TableIndexes("table1", trx)
	require.NoError(t, err)

	indexInfo, ok := indexes["id"]
	assert.True(t, ok)
	assert.EqualValues(t, `"idx1" on "table1.id" using hash [blocks: 0, records 0, distinct values: 0]`, indexInfo.String())

	indexInfo, ok = indexes["name"]
	assert.True(t, ok)
	assert.EqualValues(t, `"idx2" on "table1.name" using btree [blocks: 1, records 0, distinct values: 0]`, indexInfo.String())
}

func (ts *CommandsPlannerTestSuite) TestExecuteCreateIndex_Fail() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))
	require.NoError(t, mdm.CreateIndex("idx1", "table1", indexes.HashIndexType, "id", trx))

	_, stmt, err := parse.ParseQuery("create index idx2 on table1(id)")
	require.NoError(t, err)

	_, err = sut.ExecuteCreateIndex(
		stmt.(parse.CreateIndexStatement),
		trx,
	)
	assert.ErrorIs(t, err, planner.ErrExecuteError)

	indexes, err := mdm.TableIndexes("table1", trx)
	assert.NoError(t, err)
	assert.Len(t, indexes, 1)
}

func (ts *CommandsPlannerTestSuite) TestExecuteCreateIndex_FailIfUseCompositeKey() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("create index idx1 on table1(id, name)")
	require.NoError(t, err)

	_, err = sut.ExecuteCreateIndex(
		stmt.(parse.CreateIndexStatement),
		trx,
	)
	assert.ErrorIs(t, err, planner.ErrExecuteError)
}

func (ts *CommandsPlannerTestSuite) TestExecuteCreateView_Ok() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("create view view1 as select id from table1 where id=5")
	require.NoError(t, err)

	rows, err := sut.ExecuteCreateView(
		stmt.(parse.CreateViewStatement),
		trx,
	)
	require.NoError(t, err)
	assert.EqualValues(t, 0, rows)

	viewDef, err := mdm.ViewDef("view1", trx)
	assert.NoError(t, err)
	assert.Equal(t, "select id from table1 where id = 5", viewDef)
}

func (ts *CommandsPlannerTestSuite) TestExecuteCreateView_Fail() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))
	require.NoError(t, mdm.CreateView("view1", "select id from table1 where id=5", trx))

	_, stmt, err := parse.ParseQuery("create view view1 as select name from table1")
	require.NoError(t, err)

	_, err = sut.ExecuteCreateView(
		stmt.(parse.CreateViewStatement),
		trx,
	)
	assert.ErrorIs(t, err, planner.ErrExecuteError)
}

func (ts *CommandsPlannerTestSuite) TestExecuteDelete_Ok() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("delete from table1 where age = 5")
	require.NoError(t, err)

	// Пустая таблица
	rows, err := sut.ExecuteDelete(stmt.(parse.DeleteStatement), trx)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, rows)

	sc, err := scan.NewTableScan(trx, "table1", ts.testLayout())
	require.NoError(t, err)

	defer sc.Close()

	// Заполняем таблицу
	cnt := 100

	for i := 0; i < cnt; i++ {
		require.NoError(t, sc.Insert())
		require.NoError(t, sc.SetInt64("id", int64(i)))
	}

	ts.requireRowsCount(cnt, sc)

	// Не срабатывает предикат
	rows, err = sut.ExecuteDelete(stmt.(parse.DeleteStatement), trx)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, rows)

	for i := cnt; i < cnt*2; i++ {
		require.NoError(t, sc.Insert())
		require.NoError(t, sc.SetInt64("id", int64(i)))
		require.NoError(t, sc.SetInt8("age", int8(i%10)))
	}

	ts.requireRowsCount(cnt*2, sc)

	// Cрабатывает предикат
	rows, err = sut.ExecuteDelete(stmt.(parse.DeleteStatement), trx)
	assert.NoError(t, err)
	assert.EqualValues(t, cnt/10, rows)

	ts.requireRowsCount(cnt*2-cnt/10, sc)
}

func (ts *CommandsPlannerTestSuite) TestExecuteDelete_TableNotFound() {
	t := ts.T()

	sut, trx, _, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	_, stmt, err := parse.ParseQuery("delete from table1 where age = 5")
	require.NoError(t, err)

	rows, err := sut.ExecuteDelete(stmt.(parse.DeleteStatement), trx)
	assert.ErrorIs(t, err, planner.ErrExecuteError)
	assert.EqualValues(t, 0, rows)
}

func (ts *CommandsPlannerTestSuite) TestExecuteDelete_FieldNotFound() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("delete from table1 where field_not_found = 5")
	require.NoError(t, err)

	cnt := 100

	sc, err := scan.NewTableScan(trx, "table1", ts.testLayout())
	require.NoError(t, err)

	defer sc.Close()

	for i := 0; i < cnt; i++ {
		require.NoError(t, sc.Insert())
		require.NoError(t, sc.SetInt64("id", int64(i)))
	}

	rows, err := sut.ExecuteDelete(stmt.(parse.DeleteStatement), trx)
	assert.ErrorIs(t, err, planner.ErrExecuteError)
	assert.EqualValues(t, 0, rows)
}

func (ts *CommandsPlannerTestSuite) TestExecuteUpdate_Ok() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("update table1 set age = 99, name ='updated' where age = 5")
	require.NoError(t, err)

	// Пустая таблица
	rows, err := sut.ExecuteUpdate(stmt.(parse.UpdateStatement), trx)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, rows)

	sc, err := scan.NewTableScan(trx, "table1", ts.testLayout())
	require.NoError(t, err)

	defer sc.Close()

	// Заполняем таблицу
	cnt := 100

	for i := 0; i < cnt; i++ {
		require.NoError(t, sc.Insert())
		require.NoError(t, sc.SetInt64("id", int64(i)))
	}

	ts.requireRowsCount(cnt, sc)

	// Не срабатывает предикат
	rows, err = sut.ExecuteUpdate(stmt.(parse.UpdateStatement), trx)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, rows)

	for i := cnt; i < cnt*2; i++ {
		require.NoError(t, sc.Insert())
		require.NoError(t, sc.SetInt64("id", int64(i)))
		require.NoError(t, sc.SetInt8("age", int8(i%10)))
	}

	ts.requireRowsCount(cnt*2, sc)

	// Cрабатывает предикат
	rows, err = sut.ExecuteUpdate(stmt.(parse.UpdateStatement), trx)
	assert.NoError(t, err)
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

func (ts *CommandsPlannerTestSuite) TestExecuteUpdate_TableNotFound() {
	t := ts.T()

	sut, trx, _, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	_, stmt, err := parse.ParseQuery("update table1 set age = 99 where age = 5")
	require.NoError(t, err)

	rows, err := sut.ExecuteUpdate(stmt.(parse.UpdateStatement), trx)
	assert.ErrorIs(t, err, planner.ErrExecuteError)
	assert.EqualValues(t, 0, rows)
}

func (ts *CommandsPlannerTestSuite) TestExecuteUpdate_UpdatedFieldNotFound() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("update table1 set field_not_found = 99 where age = 5")
	require.NoError(t, err)

	cnt := 100

	sc, err := scan.NewTableScan(trx, "table1", ts.testLayout())
	require.NoError(t, err)

	defer sc.Close()

	for i := 0; i < cnt; i++ {
		require.NoError(t, sc.Insert())
		require.NoError(t, sc.SetInt64("id", int64(i)))
		require.NoError(t, sc.SetInt8("age", 5))
	}

	rows, err := sut.ExecuteUpdate(stmt.(parse.UpdateStatement), trx)
	assert.ErrorIs(t, err, planner.ErrExecuteError)
	assert.EqualValues(t, 0, rows)
}

func (ts *CommandsPlannerTestSuite) TestExecuteUpdate_PredicateFieldNotFound() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("update table1 set age = 99 where field_not_found = 5")
	require.NoError(t, err)

	cnt := 100

	sc, err := scan.NewTableScan(trx, "table1", ts.testLayout())
	require.NoError(t, err)

	defer sc.Close()

	for i := 0; i < cnt; i++ {
		require.NoError(t, sc.Insert())
		require.NoError(t, sc.SetInt64("id", int64(i)))
		require.NoError(t, sc.SetInt8("age", 5))
	}

	rows, err := sut.ExecuteUpdate(stmt.(parse.UpdateStatement), trx)
	assert.ErrorIs(t, err, planner.ErrExecuteError)
	assert.EqualValues(t, 0, rows)
}

func (ts *CommandsPlannerTestSuite) TestExecuteUpdate_MismatchFieldType() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("update table1 set age = '99' where age = 5")
	require.NoError(t, err)

	cnt := 100

	sc, err := scan.NewTableScan(trx, "table1", ts.testLayout())
	require.NoError(t, err)

	defer sc.Close()

	for i := 0; i < cnt; i++ {
		require.NoError(t, sc.Insert())
		require.NoError(t, sc.SetInt64("id", int64(i)))
		require.NoError(t, sc.SetInt8("age", 5))
	}

	rows, err := sut.ExecuteUpdate(stmt.(parse.UpdateStatement), trx)
	assert.ErrorIs(t, err, planner.ErrExecuteError)
	assert.EqualValues(t, 0, rows)
}

func (ts *CommandsPlannerTestSuite) TestExecuteInsert_Ok() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	cnt := 100
	for i := 0; i < cnt; i++ {
		_, stmt, err := parse.ParseQuery(
			fmt.Sprintf(
				"insert into table1 (id, name, age) values (%d, '%s', %d)",
				i, fmt.Sprintf("user %d", i), i%10,
			),
		)
		require.NoError(t, err)

		rows, err := sut.ExecuteInsert(stmt.(parse.InsertStatement), trx)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, rows)
	}

	sc, err := scan.NewTableScan(trx, "table1", ts.testLayout())
	require.NoError(t, err)

	defer sc.Close()

	ts.requireRowsCount(cnt, sc)

	i := 0

	require.NoError(t, scan.ForEach(sc, func() (bool, error) {
		id, err := sc.GetInt64("id")
		require.NoError(t, err)
		require.EqualValues(t, i, id)

		age, err := sc.GetInt8("age")
		require.NoError(t, err)
		require.EqualValues(t, i%10, age)

		name, err := sc.GetString("name")
		require.NoError(t, err)
		require.EqualValues(t, fmt.Sprintf("user %d", i), name)

		i++

		return false, nil
	}))
}

func (ts *CommandsPlannerTestSuite) TestExecuteInsert_UnexistantField() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("insert into table1 (id, name, years) values (1, 'name', 15)")
	require.NoError(t, err)

	rows, err := sut.ExecuteInsert(stmt.(parse.InsertStatement), trx)
	assert.ErrorIs(t, err, planner.ErrExecuteError)
	assert.EqualValues(t, 0, rows)
}

func (ts *CommandsPlannerTestSuite) TestExecuteInsert_MismatchFieldsAndValues() {
	t := ts.T()

	sut, trx, mdm, clean := ts.newSUT()
	defer clean()
	defer require.NoError(t, trx.Commit())

	require.NoError(t, mdm.CreateTable("table1", ts.testLayout().Schema, trx))

	_, stmt, err := parse.ParseQuery("insert into table1 (id, name) values (1, 'name', 15)")
	require.NoError(t, err)

	rows, err := sut.ExecuteInsert(stmt.(parse.InsertStatement), trx)
	assert.ErrorIs(t, err, planner.ErrExecuteError)
	assert.EqualValues(t, 0, rows)
}
