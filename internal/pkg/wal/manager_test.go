package wal_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

type WalManagerTestSuite struct {
	suite.Suite

	suiteDir string
}

const (
	testSuiteDir     = "wal_manager_tests"
	walFile          = "wal_log.dat"
	defaultBlockSize = 400
)

func TestWalManagerTestSuite(t *testing.T) {
	suite.Run(t, new(WalManagerTestSuite))
}

func (ts *WalManagerTestSuite) SuiteDir() string {
	return ts.suiteDir
}

func (ts *WalManagerTestSuite) SetupSuite() {
	ts.suiteDir = testutil.CreateSuiteTemporaryDir(ts, testSuiteDir)
}

func (ts *WalManagerTestSuite) TearDownSuite() {
	testutil.RemoveSuiteTemporaryDir(ts)
}

func (ts *WalManagerTestSuite) createWALManager() *wal.Manager {
	path := testutil.CreateTestTemporaryDir(ts)
	fm, err := storage.NewFileManager(path, defaultBlockSize)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	m, err := wal.NewManager(fm, walFile)
	ts.Require().NoError(err)
	ts.Require().FileExists(filepath.Join(path, walFile))

	return m
}

func (ts *WalManagerTestSuite) TestCreateManagerUnexistsLogFile() {
	m := ts.createWALManager()
	ts.Require().NotNil(m)

	defer m.StorageManager().Close()
}

func (ts *WalManagerTestSuite) TestCreateManagerExistsLogFile() {
	path := testutil.CreateTestTemporaryDir(ts)
	walPath := filepath.Join(path, walFile)

	fm, err := storage.NewFileManager(path, defaultBlockSize)
	ts.Require().NoError(err)

	defer fm.Close()

	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	p := types.NewPage(defaultBlockSize)
	p.SetUint32(0, 4)

	for i := 0; i < 2; i++ {
		_, nerr := fm.Append(walFile)
		ts.Require().NoError(nerr)
	}

	ts.Require().Equal(int64(800), testutil.GetFileSize(ts, walPath))

	nm, err := wal.NewManager(fm, walFile)
	ts.Require().NoError(err)
	ts.Equal(int32(1), nm.CurrentBlock().Number)
}

func (ts *WalManagerTestSuite) TestCreateRecords() {
	m := ts.createWALManager()
	ts.Require().NotNil(m)

	defer m.StorageManager().Close()

	for i := 0; i < 100; i++ {
		_, err := m.Append([]byte(fmt.Sprintf("record %d", i)))
		if err != nil {
			ts.FailNow(err.Error())
		}
	}
	ts.Equal(int64(1600), testutil.GetFileSize(ts, filepath.Join(m.StorageManager().Path(), walFile)))

	it, err := m.Iterator()
	ts.Require().NoError(err)

	i := 99

	for it.HasNext() {
		d, err := it.Next()
		if err != nil {
			ts.FailNow(err.Error())
		}

		ts.Equal(fmt.Sprintf("record %d", i), string(d))
		i--
	}
}
