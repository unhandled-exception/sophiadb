package buffers_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

type ManagerTestSuite struct {
	suite.Suite
	suiteDir string
}

func TestManagerTestSuite(t *testing.T) {
	suite.Run(t, new(ManagerTestSuite))
}

func (ts *ManagerTestSuite) SuiteDir() string {
	return ts.suiteDir
}

func (ts *ManagerTestSuite) SetupSuite() {
	testSuiteDir := "buffers_manager_tests"
	ts.suiteDir = testutil.CreateSuiteTemporaryDir(ts, testSuiteDir)
}

func (ts *ManagerTestSuite) TearDownSuite() {
	testutil.RemoveSuiteTemporaryDir(ts)
}

func (ts *ManagerTestSuite) createBuffersManager(pLen int) (*buffers.Manager, string) {
	var defaultBlockSize uint32 = 400

	walFile := "wal_log.dat"

	path := testutil.CreateTestTemporaryDir(ts)
	fm, err := storage.NewFileManager(path, defaultBlockSize)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	lm, err := wal.NewManager(fm, walFile)
	ts.Require().NoError(err)
	ts.Require().FileExists(filepath.Join(path, walFile))

	m := buffers.NewManager(fm, lm, pLen)
	ts.Require().NotNil(m)

	m.SetMaxPinLockTime(250 * time.Millisecond)

	return m, path
}

func (ts *ManagerTestSuite) TestBuffersManager() {
	bm, path := ts.createBuffersManager(3)
	defer bm.StorageManager().Close()

	testFile := "test_file.dat"

	var bufs [7]*buffers.Buffer

	var err error

	testutil.CreateFile(ts, filepath.Join(path, testFile), make([]byte, 10*400))

	// Занимаем все буферы в пуле
	bufs[0], err = bm.Pin(storage.NewBlockID(testFile, 0))
	ts.Require().NoError(err)
	ts.NotNil(bufs[0])
	ts.Equal(make([]byte, 400), bufs[0].Content().Content())

	bufs[1], err = bm.Pin(storage.NewBlockID(testFile, 1))
	ts.Require().NoError(err)
	ts.NotNil(bufs[1])

	bufs[2], err = bm.Pin(storage.NewBlockID(testFile, 2))
	ts.Require().NoError(err)
	ts.NotNil(bufs[2])

	// Освобождаем один буфер
	bm.Unpin(bufs[1])
	ts.False(bufs[1].IsPinned())
	bufs[1] = nil

	// Получаем еще одну ссылку на запиненный буфер
	bufs[3], err = bm.Pin(storage.NewBlockID(testFile, 0))
	ts.Require().NoError(err)
	ts.NotNil(bufs[3])
	ts.Equal(2, bufs[3].Pins())

	// Используем существующий буфер не создавая новый
	bufs[4], err = bm.Pin(storage.NewBlockID(testFile, 1))
	ts.Require().NoError(err)
	ts.Equal(1, bufs[4].Pins())

	ts.Equal(0, bm.Available())

	// Пытаемся занять пул новым блоком и получаем ошибку
	bufs[5], err = bm.Pin(storage.NewBlockID(testFile, 3))
	ts.Require().ErrorIs(err, buffers.ErrNoAvailableBuffers)
	ts.Nil(bufs[5])

	// Освобождаем один из буферов и снова пытаемся занять
	bm.Unpin(bufs[2])
	bufs[2] = nil

	ts.Equal(1, bm.Available())

	// Пытаемся запинить несуществующий на диске блок
	bufs[5], err = bm.Pin(storage.NewBlockID(testFile, 1000))
	ts.Require().ErrorIs(err, buffers.ErrFailedToAssignBlockToBuffer)
	ts.Nil(bufs[5])

	// Теперь запиниваем нрмальный блок
	bufs[5], err = bm.Pin(storage.NewBlockID(testFile, 3))
	ts.Require().NoError(err)
	ts.NotNil(bufs[5])

	// Сбрасываем буферы на диск
	bufs[0].SetModified(1, 2)
	bufs[5].SetModified(2, 3)
	ts.Equal(int64(1), bufs[0].ModifyingTX())
	ts.Equal(int64(2), bufs[0].LSN())
	ts.Equal(int64(2), bufs[5].ModifyingTX())
	ts.Equal(int64(3), bufs[5].LSN())

	err = bm.FlushAll(1)
	ts.Require().NoError(err)
	ts.Equal(int64(-1), bufs[0].ModifyingTX())
	ts.Equal(int64(2), bufs[5].ModifyingTX())
}
