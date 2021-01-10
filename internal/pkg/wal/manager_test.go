package wal

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/test"
)

type ManagerTestSuite struct {
	suite.Suite
	suiteDir string
}

const (
	walFile          = "wal_log.dat"
	defaultBlockSize = 400
)

func TestManagerTestSuite(t *testing.T) {
	suite.Run(t, new(ManagerTestSuite))
}

func (ts *ManagerTestSuite) SuiteDir() string {
	return ts.suiteDir
}

func (ts *ManagerTestSuite) SetupSuite() {
	ts.suiteDir = test.CreateSuiteTemporaryDir(ts, "file_manager_tests_")
}

func (ts *ManagerTestSuite) TearDownSuite() {
	test.RemoveSuiteTemporaryDir(ts)
}

func (ts *ManagerTestSuite) createWALManager(testName string) *Manager {
	path := test.CreateTestTemporaryDir(ts, testName)
	fm, err := storage.NewFileManager(path, defaultBlockSize)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	m, err := NewManager(fm, walFile)
	ts.Require().NoError(err)
	ts.Require().FileExists(filepath.Join(path, walFile))
	return m
}

func (ts *ManagerTestSuite) TestCreateManagerUnexistsLogFile() {
	m := ts.createWALManager("TestCreateManagerUnexistsLogFile_")
	ts.Require().NotNil(m)
	defer m.fm.Close()
}

func (ts *ManagerTestSuite) TestCreateManagerExistsLogFile() {
	testName := "TestCreateManagerExistsLogFile_"
	path := test.CreateTestTemporaryDir(ts, testName)
	walPath := filepath.Join(path, walFile)
	fm, err := storage.NewFileManager(path, defaultBlockSize)
	defer fm.Close()

	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	p := storage.NewPage(defaultBlockSize)
	p.SetUint32(0, 4)

	for i := 0; i < 2; i++ {
		_, err := fm.Append(walFile)
		ts.Require().NoError(err)
	}
	ts.Require().Equal(int64(800), test.GetFileSize(ts, walPath))

	nm, err := NewManager(fm, walFile)
	ts.Require().NoError(err)
	ts.Equal(uint32(1), nm.currentBlock.Number())
}

func (ts *ManagerTestSuite) TestCreateRecords() {
	m := ts.createWALManager("TestCreateRecords_")
	ts.Require().NotNil(m)
	defer m.fm.Close()

	for i := 0; i < 100; i++ {
		_, err := m.Append([]byte(fmt.Sprintf("record %d", i)))
		if err != nil {
			ts.FailNow(err.Error())
		}
	}
	ts.Equal(int64(1600), test.GetFileSize(ts, filepath.Join(m.fm.Path(), walFile)))

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
