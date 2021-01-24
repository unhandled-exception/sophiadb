package buffers

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/rotisserie/eris"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/test"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

type ManagerTestSuite struct {
	suite.Suite
	suiteDir string
}

const (
	testSuiteDir     = "buffers_manager_tests"
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
	ts.suiteDir = test.CreateSuiteTemporaryDir(ts, testSuiteDir)
}

func (ts *ManagerTestSuite) TearDownSuite() {
	test.RemoveSuiteTemporaryDir(ts)
}

func (ts *ManagerTestSuite) createBuffersManager(len int) (*Manager, string) {
	path := test.CreateTestTemporaryDir(ts)
	fm, err := storage.NewFileManager(path, defaultBlockSize)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	lm, err := wal.NewManager(fm, walFile)
	ts.Require().NoError(err)
	ts.Require().FileExists(filepath.Join(path, walFile))

	m := NewManager(fm, lm, len)
	ts.Require().NotNil(m)

	m.SetMaxPinLockTime(250 * time.Millisecond)

	return m, path
}

func (ts *ManagerTestSuite) TestBuffersManager() {
	bm, path := ts.createBuffersManager(3)
	defer bm.fm.Close()

	testFile := "testFile.dat"
	var buffers [7]*Buffer
	var err error

	test.CreateFile(ts, filepath.Join(path, testFile), make([]byte, 10*400))

	// Занимаем все буферы в пуле
	buffers[0], err = bm.Pin(storage.NewBlockID(testFile, 0))
	ts.Require().NoError(err)
	ts.NotNil(buffers[0])
	ts.Equal(make([]byte, 400), buffers[0].Content().Content())

	buffers[1], err = bm.Pin(storage.NewBlockID(testFile, 1))
	ts.Require().NoError(err)
	ts.NotNil(buffers[1])

	buffers[2], err = bm.Pin(storage.NewBlockID(testFile, 2))
	ts.Require().NoError(err)
	ts.NotNil(buffers[2])

	// Освобождаем один буфер
	bm.Unpin(buffers[1])
	ts.False(buffers[1].IsPinned())
	buffers[1] = nil

	// Получаем еще одну ссылку на запиненный буфер
	buffers[3], err = bm.Pin(storage.NewBlockID(testFile, 0))
	ts.Require().NoError(err)
	ts.NotNil(buffers[3])
	ts.Equal(2, buffers[3].pins)

	// Используем существующий буфер не создавая новый
	buffers[4], err = bm.Pin(storage.NewBlockID(testFile, 1))
	ts.Require().NoError(err)
	ts.Equal(1, buffers[4].pins)

	ts.Equal(0, bm.Available())

	// Пытаемся занять пул новым блоком и получаем ошибку
	buffers[5], err = bm.Pin(storage.NewBlockID(testFile, 3))
	ts.Require().True(eris.Is(err, NoAvailableBuffers), err.Error())
	ts.Nil(buffers[5])

	// Освобождаем один из буферов и снова пытаемся занять
	bm.Unpin(buffers[2])
	buffers[2] = nil
	ts.Equal(1, bm.Available())

	// Пытаемся запинить несуществующий на диске блок
	buffers[5], err = bm.Pin(storage.NewBlockID(testFile, 1000))
	ts.Require().True(eris.Is(err, FailedToAssignBlockToBuffer), err.Error())
	ts.Nil(buffers[5])

	// Теперь запиниваем нрмальный блок
	buffers[5], err = bm.Pin(storage.NewBlockID(testFile, 3))
	ts.Require().NoError(err)
	ts.NotNil(buffers[5])

	// Сбрасываем буферы на диск
	buffers[0].SetModified(1, 2)
	buffers[5].SetModified(2, 3)
	ts.Equal(int64(1), buffers[0].ModifyingTX())
	ts.Equal(int64(2), buffers[0].lsn)
	ts.Equal(int64(2), buffers[5].ModifyingTX())
	ts.Equal(int64(3), buffers[5].lsn)

	err = bm.FlushAll(1)
	ts.Require().NoError(err)
	ts.Equal(int64(-1), buffers[0].ModifyingTX())
	ts.Equal(int64(2), buffers[5].ModifyingTX())
}
