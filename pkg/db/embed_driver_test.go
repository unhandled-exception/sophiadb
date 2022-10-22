package db_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/db"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
)

type EmbedDriverTestSuite struct {
	suite.Suite
}

func TestEmbedDriverTestSuite(t *testing.T) {
	suite.Run(t, new(EmbedDriverTestSuite))
}

func (ts *EmbedDriverTestSuite) newConnSUT() (*sql.Conn, func()) {
	t := ts.T()

	dbPath := t.TempDir()
	ctx := context.Background()

	db, err := sql.Open(db.EmbedDriverName, dbPath)
	require.NoError(t, err)
	require.NotNil(t, db)

	sut, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, sut)

	return sut, func() {
		assert.NoError(t, sut.Close())
		assert.NoError(t, db.Close())
	}
}

func (ts *EmbedDriverTestSuite) TestNewEmbedDriverConnection_WithoutOptions() {
	t := ts.T()

	dbPath := t.TempDir()
	ctx := context.Background()

	db, err := sql.Open(db.EmbedDriverName, dbPath)
	require.NoError(t, err)
	assert.NotNil(t, db)

	conn1, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn1)

	require.NoError(t, conn1.PingContext(ctx))

	conn2, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	require.NoError(t, db.Close())
}

func (ts *EmbedDriverTestSuite) TestExec() {
	t := ts.T()

	ctx := context.Background()

	sut, clean := ts.newConnSUT()
	defer clean()

	_, err := sut.ExecContext(ctx, "create table table1 (id int64, name varchar(100), age int8)")
	require.NoError(t, err)

	_, err = sut.ExecContext(ctx, "select id from table1")
	require.ErrorIs(t, err, parse.ErrInvalidStatement)

	res, err := sut.ExecContext(ctx, "insert into table1 (id, name, age) values (1, 'name 1', 2)")
	require.NoError(t, err)

	rows, _ := res.RowsAffected()
	assert.EqualValues(t, 1, rows)
}

func (ts *EmbedDriverTestSuite) TestQuery() {
	t := ts.T()

	ctx := context.Background()

	sut, clean := ts.newConnSUT()
	defer clean()

	_, err := sut.ExecContext(ctx, "create table table1 (id int64, name varchar(100), age int8)")
	require.NoError(t, err)

	cnt := 1000

	for i := 0; i < cnt; i++ {
		res, err1 := sut.ExecContext(ctx, fmt.Sprintf("insert into table1 (id, name, age) values (%d, 'name %d', %d)", i, i, i%127))
		assert.NoError(t, err1)

		rows, _ := res.RowsAffected()
		assert.EqualValues(t, 1, rows)
	}

	type qres struct {
		id   int64
		name string
		age  int8
	}

	res := qres{}

	err = sut.QueryRowContext(ctx, "select id, name, age from table1 where id = 1").Scan(&res.id, &res.name, &res.age)
	require.NoError(t, err)

	assert.Equal(t, qres{id: 1, name: "name 1", age: 1}, res)

	rows, err := sut.QueryContext(ctx, "select id, name, age from table1")
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, rows.Close())
	}()

	i := 0

	for rows.Next() {
		var mres qres

		err := rows.Scan(&mres.id, &mres.name, &mres.age)
		require.NoError(t, err)

		assert.Equal(t,
			qres{id: int64(i), name: "name " + strconv.Itoa(i), age: int8(i % 127)},
			mres,
		)

		i++
	}

	assert.NoError(t, rows.Err())

	assert.EqualValues(t, cnt, i)
}

func (ts *EmbedDriverTestSuite) TestTransaction_Ok() {
	t := ts.T()

	ctx := context.Background()

	sut, clean := ts.newConnSUT()
	defer clean()

	_, err := sut.ExecContext(ctx, "create table table1 (id int64, name varchar(100), age int8)")
	require.NoError(t, err)

	cnt := 10

	tx, err := sut.BeginTx(ctx, nil)
	require.NoError(t, err)

	for i := 0; i < cnt; i++ {
		res, err1 := tx.ExecContext(ctx, fmt.Sprintf("insert into table1 (id, name, age) values (%d, 'name %d', %d)", i, i, i%255))
		assert.NoError(t, err1)

		rows, _ := res.RowsAffected()
		assert.EqualValues(t, 1, rows)
	}

	require.NoError(t, tx.Commit())

	// TODO: доделать тест с запросом
	_ = false
}
