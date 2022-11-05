package db_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/parse"
	"github.com/unhandled-exception/sophiadb/pkg/db"
)

type EmbedDriverTestSuite struct {
	suite.Suite
}

func TestEmbedDriverTestSuite(t *testing.T) {
	suite.Run(t, new(EmbedDriverTestSuite))
}

func (ts *EmbedDriverTestSuite) newDB() (*sql.DB, func()) {
	t := ts.T()

	dbPath := t.TempDir()

	db, err := sql.Open(db.EmbedDriverName, dbPath)
	require.NoError(t, err)
	require.NotNil(t, db)

	return db, func() {
		assert.NoError(t, db.Close())
	}
}

func (ts *EmbedDriverTestSuite) newConnSUT() (*sql.Conn, func()) {
	t := ts.T()

	ctx := context.Background()

	db, cleanDB := ts.newDB()

	sut, err := db.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, sut)

	return sut, func() {
		assert.NoError(t, sut.Close())
		cleanDB()
	}
}

func (ts *EmbedDriverTestSuite) TestNewEmbedDriverConnection_WithoutOptions() {
	t := ts.T()

	dbPath := t.TempDir()
	ctx := context.Background()

	edb, err := sql.Open(db.EmbedDriverName, dbPath)
	require.NoError(t, err)
	assert.NotNil(t, edb)

	conn1, err := edb.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn1)

	_ = conn1.Raw(func(driverConn any) error {
		rdb, ok := driverConn.(interface{ DB() *db.Database })
		require.True(t, ok)

		assert.Equal(t, dbPath, rdb.DB().DataDir())
		assert.EqualValues(t, db.DefaultBlockSize, rdb.DB().BlockSize())
		assert.EqualValues(t, db.DefaultLogFilename, rdb.DB().LogFileName())
		assert.EqualValues(t, db.DefaultBuffersPoolLen, rdb.DB().BuffersPoolLen())
		assert.EqualValues(t, db.DefaultPinLockTimeout, rdb.DB().PinLockTimeout())
		assert.EqualValues(t, db.DefaultTransactionLockTimeout, rdb.DB().TransactionLockTimeout())

		return nil
	})

	require.NoError(t, conn1.PingContext(ctx))

	conn2, err := edb.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	require.NoError(t, edb.Close())
}

func (ts *EmbedDriverTestSuite) TestNewEmbedDriverConnection_BadDSN() {
	t := ts.T()

	path := t.TempDir()

	tests := []struct {
		dsn  string
		err  error
		desc string
	}{
		{path + "?;", db.ErrBadDSN, ""},
		{path + "?block_size", db.ErrBadDSN, "bad uint32 value: strconv.ParseUint: parsing \"\": invalid syntax: bad DSN"},
		{path + "?block_size=123&name_no_value", db.ErrBadDSN, "unknown key: name_no_value: bad DSN"},
		{"", db.ErrBadDSN, "empty path: bad DSN"},
		{path + "?block_size=ddd", db.ErrBadDSN, "bad uint32 value: strconv.ParseUint: parsing \"ddd\": invalid syntax: bad DSN"},
		{path + "?buffers_pool_len=ddd", db.ErrBadDSN, "bad int value: strconv.ParseInt: parsing \"ddd\": invalid syntax: bad DSN"},
		{path + "?pin_lock_timeout=24", db.ErrBadDSN, "bad duration value: time: missing unit in duration \"24\": bad DSN"},
		{path + "?transaction_lock_timeout=35", db.ErrBadDSN, "bad duration value: time: missing unit in duration \"35\": bad DSN"},
	}

	for _, tc := range tests {
		db, err := sql.Open(db.EmbedDriverName, tc.dsn)
		require.NoError(t, err)

		_, err = db.Conn(context.Background())
		if err == nil {
			t.Logf("test '%s' has no errors", tc.dsn)
			t.Fail()

			continue
		}

		assert.ErrorIs(t, err, tc.err)

		if tc.desc != "" {
			assert.Equal(t, tc.desc, err.Error())
		}
	}
}

func (ts *EmbedDriverTestSuite) TestNewEmbedDirverConnection_WithOption() {
	t := ts.T()

	dbPath := t.TempDir()
	ctx := context.Background()

	edb, err := sql.Open(
		db.EmbedDriverName,
		dbPath+
			"?block_size=15000"+
			"&log_file_name=new_wal.log"+
			"&buffers_pool_len=12345"+
			"&pin_lock_timeout=4m"+
			"&transaction_lock_timeout=25s",
	)
	require.NoError(t, err)
	assert.NotNil(t, edb)

	conn, err := edb.Conn(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn)

	require.NoError(t, conn.PingContext(ctx))

	require.NoError(t, edb.Close())

	_ = conn.Raw(func(driverConn any) error {
		rdb, ok := driverConn.(interface{ DB() *db.Database })
		require.True(t, ok)

		assert.EqualValues(t, dbPath, rdb.DB().DataDir())
		assert.EqualValues(t, 15000, rdb.DB().BlockSize())
		assert.EqualValues(t, "new_wal.log", rdb.DB().LogFileName())
		assert.EqualValues(t, 12345, rdb.DB().BuffersPoolLen())
		assert.EqualValues(t, 4*time.Minute, rdb.DB().PinLockTimeout())
		assert.EqualValues(t, 25*time.Second, rdb.DB().TransactionLockTimeout())

		return nil
	})
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

	tx1, err := sut.BeginTx(ctx, nil)
	require.NoError(t, err)

	for i := 0; i < cnt; i++ {
		res, err1 := tx1.ExecContext(ctx, fmt.Sprintf("insert into table1 (id, name, age) values (%d, 'name %d', %d)", i, i, i%255))
		assert.NoError(t, err1)

		rows, _ := res.RowsAffected()
		assert.EqualValues(t, 1, rows)
	}

	require.NoError(t, tx1.Commit())

	tx2, err := sut.BeginTx(ctx, nil)
	require.NoError(t, err)

	row2 := scanRowToRecord(t, tx2.QueryRowContext(ctx, "select id, name, age from table1 where id = 5"))
	assert.Equal(t, "name 5", row2.Name)

	_, err = tx2.ExecContext(ctx, "update table1 set name = 'new name 5' where id = 5")
	assert.NoError(t, err)

	require.NoError(t, tx2.Commit())

	tx3, err := sut.BeginTx(ctx, nil)
	require.NoError(t, err)

	row2 = scanRowToRecord(t, tx3.QueryRowContext(ctx, "select id, name, age from table1 where id = 5"))
	assert.Equal(t, "new name 5", row2.Name)

	require.NoError(t, tx3.Commit())
}

func (ts *EmbedDriverTestSuite) TestTransaction_Rollback() {
	t := ts.T()

	ctx := context.Background()

	sut, clean := ts.newConnSUT()
	defer clean()

	_, err := sut.ExecContext(ctx, "create table table1 (id int64, name varchar(100), age int8)")
	require.NoError(t, err)

	cnt := 10

	tx1, err := sut.BeginTx(ctx, nil)
	require.NoError(t, err)

	for i := 0; i < cnt; i++ {
		res, err1 := tx1.ExecContext(ctx, fmt.Sprintf("insert into table1 (id, name, age) values (%d, 'name %d', %d)", i, i, i%255))
		assert.NoError(t, err1)

		rows, _ := res.RowsAffected()
		assert.EqualValues(t, 1, rows)
	}

	require.NoError(t, tx1.Commit())

	tx2, err := sut.BeginTx(ctx, nil)
	require.NoError(t, err)

	row2 := scanRowToRecord(t, tx2.QueryRowContext(ctx, "select id, name, age from table1 where id = 5"))
	assert.Equal(t, "name 5", row2.Name)

	_, err = tx2.ExecContext(ctx, "update table1 set name = 'new name 5' where id = 5")
	assert.NoError(t, err)

	require.NoError(t, tx2.Rollback())

	tx3, err := sut.BeginTx(ctx, nil)
	require.NoError(t, err)

	row2 = scanRowToRecord(t, tx3.QueryRowContext(ctx, "select id, name, age from table1 where id = 5"))
	assert.Equal(t, "name 5", row2.Name)

	require.NoError(t, tx3.Commit())
}

func (ts *EmbedDriverTestSuite) TestTransaction_MultipleConnections() {
	t := ts.T()

	ctx := context.Background()

	db, cleanDB := ts.newDB()
	defer cleanDB()

	con1, err := db.Conn(ctx)
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, con1.Close())
	}()

	con2, err := db.Conn(ctx)
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, con2.Close())
	}()

	con3, err := db.Conn(ctx)
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, con3.Close())
	}()

	_, err = con1.ExecContext(ctx, "create table table1 (id int64, name varchar(100), age int8)")
	require.NoError(t, err)

	cnt := 10

	tx1, err := con1.BeginTx(ctx, nil)
	require.NoError(t, err)

	tx2, err := con2.BeginTx(ctx, nil)
	require.NoError(t, err)

	tx3, err := con3.BeginTx(ctx, nil)
	require.NoError(t, err)

	for i := 0; i < cnt; i++ {
		res, err1 := tx1.ExecContext(ctx, fmt.Sprintf("insert into table1 (id, name, age) values (%d, 'name %d', %d)", i, i, i%255))
		assert.NoError(t, err1)

		rows, _ := res.RowsAffected()
		assert.EqualValues(t, 1, rows)
	}

	require.NoError(t, tx1.Commit())

	row2 := scanRowToRecord(t, tx2.QueryRowContext(ctx, "select id, name, age from table1 where id = 5"))
	assert.Equal(t, "name 5", row2.Name)

	_, err = tx2.ExecContext(ctx, "update table1 set name = 'new name 5' where id = 5")
	assert.NoError(t, err)

	require.NoError(t, tx2.Commit())

	row2 = scanRowToRecord(t, tx3.QueryRowContext(ctx, "select id, name, age from table1 where id = 5"))
	assert.Equal(t, "new name 5", row2.Name)

	require.NoError(t, tx3.Commit())
}

func (ts *EmbedDriverTestSuite) TestStartTransactionAlreadyStarted() {
	t := ts.T()

	ctx := context.Background()

	sut, clean := ts.newConnSUT()
	defer clean()

	tx1, err := sut.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = sut.BeginTx(ctx, nil)
	assert.ErrorIs(t, err, db.ErrTransactionAlreadyStarted)

	require.NoError(t, tx1.Commit())
}

func (ts *EmbedDriverTestSuite) TestPlaceholders_Ok() {
	t := ts.T()

	ctx := context.Background()

	sut, clean := ts.newConnSUT()
	defer clean()

	var err error

	_, err = sut.ExecContext(ctx, "create table table1 (id int64, name varchar(100), age int8)")
	require.NoError(t, err)

	_, err = sut.ExecContext(ctx, "insert into table1 (id, name, age) values (?, ?, ?)", 1, "name '1'", 15)
	assert.NoError(t, err)

	rows, err := sut.QueryContext(ctx, "select id, name, age from table1 where id = ? and name=:name and age = ?", 1, sql.Named("name", "name '1'"), 15)
	assert.NoError(t, err)
	assert.NoError(t, rows.Err())
	assert.NoError(t, rows.Close()) //nolint:sqlclosecheck

	_, err = sut.ExecContext(ctx, "insert into table1 (id, name, age) values (:id, :name, :age)",
		sql.Named("id", 2),
		sql.Named("name", "name '2'"),
		sql.Named("age", 25),
	)
	assert.NoError(t, err)
}

func (ts *EmbedDriverTestSuite) TestPlaceholders_Errors() {
	t := ts.T()

	ctx := context.Background()

	sut, clean := ts.newConnSUT()
	defer clean()

	var err error

	_, err = sut.ExecContext(ctx, "insert into table1 (id, name) values (?, ?)")
	assert.ErrorIs(t, err, db.ErrFailedProcessPlaceholders)

	_, err = sut.ExecContext(ctx, "insert into table1 (id, name) values (?, ?)", sql.Named("id", 1), "name 1")
	assert.ErrorIs(t, err, db.ErrFailedProcessPlaceholders)

	_, err = sut.ExecContext(ctx, "insert into table1 (id, name) values (?, ?)", 10, 10.45)
	assert.ErrorIs(t, err, db.ErrUnserializableValue)

	_, err = sut.ExecContext(ctx, "insert into table1 (id, name) values (:id, :name)")
	assert.ErrorIs(t, err, db.ErrFailedProcessPlaceholders)

	_, err = sut.ExecContext(ctx, "insert into table1 (id, name) values (:id, :name)", sql.Named("id", 1), sql.Named("username", "name 1"))
	assert.ErrorIs(t, err, db.ErrFailedProcessPlaceholders)

	_, err = sut.ExecContext(ctx, "insert into table1 (id, name) values (:id, :name?)", sql.Named("id", 1), sql.Named("username", "name 1"))
	assert.ErrorIs(t, err, db.ErrFailedProcessPlaceholders)
}
