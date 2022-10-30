package db_test

import (
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/pkg/db"
)

const (
	testDataDir = "data"

	testWOBlockSize      = 3 * 1024
	testWOLogFileName    = "custom_wal.log"
	testWOBuffersPoolLen = 123
)

var (
	testWOPinLockTimeout         time.Duration = 13 * time.Second
	testWOTransactionLockTimeout time.Duration = 15 * time.Second
)

type DatabaseTestSuite struct {
	suite.Suite
}

func TestDatabaseTestSuite(t *testing.T) {
	suite.Run(t, new(DatabaseTestSuite))
}

func (ts *DatabaseTestSuite) TestNewDatabase_Default() {
	t := ts.T()
	path := path.Join(t.TempDir(), testDataDir)

	sut, err := db.NewDatabase(path)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, sut.Close())
	}()

	assert.True(t, sut.IsNew())
	assert.EqualValues(t, path, sut.DataDir())
	assert.EqualValues(t, db.DefaultBlockSize, sut.BlockSize())
	assert.EqualValues(t, db.DefaultLogFilename, sut.LogFileName())
	assert.EqualValues(t, db.DefaultBuffersPoolLen, sut.BuffersPoolLen())
	assert.EqualValues(t, db.DefaultPinLockTimeout, sut.PinLockTimeout())
	assert.EqualValues(t, db.DefaultTransactionLockTimeout, sut.TransactionLockTimeout())
}

func (ts *DatabaseTestSuite) TestNewDatabase_WithOptions() {
	t := ts.T()
	path := path.Join(t.TempDir(), testDataDir)

	sut, err := db.NewDatabase(
		path,
		db.WithBlockSize(testWOBlockSize),
		db.WithLogFileName(testWOLogFileName),
		db.WithBuffersPoolLen(testWOBuffersPoolLen),
		db.WithPinLockTimeout(testWOPinLockTimeout),
		db.WithTransactionLockTimeout(testWOTransactionLockTimeout),
	)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, sut.Close())
	}()

	assert.True(t, sut.IsNew())
	assert.EqualValues(t, path, sut.DataDir())
	assert.EqualValues(t, testWOBlockSize, sut.BlockSize())
	assert.EqualValues(t, testWOLogFileName, sut.LogFileName())
	assert.EqualValues(t, testWOBuffersPoolLen, sut.BuffersPoolLen())
	assert.EqualValues(t, testWOPinLockTimeout, sut.PinLockTimeout())
	assert.EqualValues(t, testWOTransactionLockTimeout, sut.TransactionLockTimeout())
}

func (ts *DatabaseTestSuite) TestNewDatabase_ExistsDatabase() {
	t := ts.T()
	path := path.Join(t.TempDir(), testDataDir)

	sdb, err := db.NewDatabase(path)
	require.NoError(t, err)
	require.NoError(t, sdb.Close())

	sut, err := db.NewDatabase(path)
	require.NoError(t, err)

	assert.False(t, sut.IsNew())

	require.NoError(t, sut.Close())
}

func (ts *DatabaseTestSuite) TestPlanner() {
	t := ts.T()
	path := path.Join(t.TempDir(), testDataDir)

	db, err := db.NewDatabase(path)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, db.Close())
	}()

	sut := db.Planner()
	require.NotNil(t, sut)

	trx, err := db.Transaction()
	require.NoError(t, err)
	require.NotNil(t, trx)

	defer require.NoError(t, trx.Commit())

	_, err = sut.ExecuteCommand("create table table1 (id int64, name varchar(100))", trx)
	assert.NoError(t, err)

	rows, err := sut.ExecuteCommand("insert into table1 (id, name) values (1, 'user 1')", trx)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, rows)

	qp, err := sut.CreateQueryPlan("select id from table1", trx)
	assert.NoError(t, err)

	sc, err := qp.Open()
	require.NoError(t, err)

	require.NoError(t, scan.ForEach(sc, func() (bool, error) {
		id, err := sc.GetInt64("id")
		assert.NoError(t, err)
		assert.EqualValues(t, 1, id)

		_, err = sc.GetString("name")
		assert.ErrorIs(t, err, scan.ErrFieldNotFound)

		return false, nil
	}))
}
