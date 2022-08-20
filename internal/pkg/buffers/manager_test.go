package buffers_test

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

const testFile = "test_file.dat"

type BuffersManagerTestSuite struct {
	suite.Suite
	suiteDir string
}

func TestManagerTestSuite(t *testing.T) {
	suite.Run(t, new(BuffersManagerTestSuite))
}

func (ts *BuffersManagerTestSuite) SuiteDir() string {
	return ts.suiteDir
}

func (ts *BuffersManagerTestSuite) SetupSuite() {
	testSuiteDir := "buffers_manager_tests"
	ts.suiteDir = testutil.CreateSuiteTemporaryDir(ts, testSuiteDir)
}

func (ts *BuffersManagerTestSuite) TearDownSuite() {
	testutil.RemoveSuiteTemporaryDir(ts)
}

func (ts *BuffersManagerTestSuite) createBuffersManager(pLen int, opts ...buffers.ManagerOpt) (*buffers.Manager, string) {
	var defaultBlockSize uint32 = 400

	walFile := "wal_log.dat"

	path := testutil.CreateTestTemporaryDir(ts)
	fm, err := storage.NewFileManager(path, defaultBlockSize)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	lm, err := wal.NewManager(fm, walFile)
	ts.Require().NoError(err)
	ts.Require().FileExists(filepath.Join(path, walFile))

	m := buffers.NewManager(fm, lm, pLen, opts...)
	ts.Require().NotNil(m)

	m.SetMaxPinLockTime(250 * time.Millisecond)

	return m, path
}

func (ts *BuffersManagerTestSuite) TestWaitToPinBuffer() {
	t := ts.T()

	sut, path := ts.createBuffersManager(1, buffers.WithPinLockTimeout(100*time.Millisecond))
	defer sut.StorageManager().Close()

	testutil.CreateFile(ts, filepath.Join(path, testFile), make([]byte, 10*400))

	block1 := types.Block{Filename: testFile, Number: 1}
	block2 := types.Block{Filename: testFile, Number: 2}

	buf1, err := sut.Pin(block1)
	require.NoError(t, err)

	_, err = sut.Pin(block2)
	require.ErrorIs(t, err, buffers.ErrNoAvailableBuffers)

	go func() {
		time.Sleep(30 * time.Millisecond)
		sut.Unpin(buf1)
	}()

	_, err = sut.Pin(block2)
	require.NoError(t, err)
}

func (ts *BuffersManagerTestSuite) TestBuffersManager() {
	bm, path := ts.createBuffersManager(3)
	defer bm.StorageManager().Close()

	var bufs [7]*buffers.Buffer

	var err error

	testutil.CreateFile(ts, filepath.Join(path, testFile), make([]byte, 10*400))

	// Занимаем все буферы в пуле
	bufs[0], err = bm.Pin(types.Block{Filename: testFile, Number: 0})
	ts.Require().NoError(err)
	ts.NotNil(bufs[0])
	ts.Equal(make([]byte, 400), bufs[0].Content().Content())

	bufs[1], err = bm.Pin(types.Block{Filename: testFile, Number: 1})
	ts.Require().NoError(err)
	ts.NotNil(bufs[1])

	bufs[2], err = bm.Pin(types.Block{Filename: testFile, Number: 2})
	ts.Require().NoError(err)
	ts.NotNil(bufs[2])

	// Освобождаем один буфер
	bm.Unpin(bufs[1])
	ts.False(bufs[1].IsPinned())
	bufs[1] = nil

	// Получаем еще одну ссылку на запиненный буфер
	bufs[3], err = bm.Pin(types.Block{Filename: testFile, Number: 0})
	ts.Require().NoError(err)
	ts.NotNil(bufs[3])
	ts.Equal(2, bufs[3].Pins())

	// Используем существующий буфер не создавая новый
	bufs[4], err = bm.Pin(types.Block{Filename: testFile, Number: 1})
	ts.Require().NoError(err)
	ts.Equal(1, bufs[4].Pins())

	ts.Equal(0, bm.Available())

	// Пытаемся занять пул новым блоком и получаем ошибку
	bufs[5], err = bm.Pin(types.Block{Filename: testFile, Number: 3})
	ts.Require().ErrorIs(err, buffers.ErrNoAvailableBuffers)
	ts.Nil(bufs[5])

	// Освобождаем один из буферов и снова пытаемся занять
	bm.Unpin(bufs[2])
	bufs[2] = nil

	ts.Equal(1, bm.Available())

	// Пытаемся запинить несуществующий на диске блок
	bufs[5], err = bm.Pin(types.Block{Filename: testFile, Number: 1000})
	ts.Require().ErrorIs(err, buffers.ErrFailedToAssignBlockToBuffer)
	ts.Nil(bufs[5])

	// Теперь запиниваем нормальный блок
	bufs[5], err = bm.Pin(types.Block{Filename: testFile, Number: 3})
	ts.Require().NoError(err)
	ts.NotNil(bufs[5])

	// Сбрасываем буферы на диск
	bufs[0].SetModified(1, 2)
	bufs[5].SetModified(2, 3)
	ts.Equal(types.TRX(1), bufs[0].ModifyingTX())
	ts.Equal(types.LSN(2), bufs[0].LSN())
	ts.Equal(types.TRX(2), bufs[5].ModifyingTX())
	ts.Equal(types.LSN(3), bufs[5].LSN())

	err = bm.FlushAll(1)
	ts.Require().NoError(err)
	ts.Equal(types.TRX(-1), bufs[0].ModifyingTX())
	ts.Equal(types.TRX(2), bufs[5].ModifyingTX())
}

func (ts *BuffersManagerTestSuite) TestConcurrency() {
	t := ts.T()

	sut, path := ts.createBuffersManager(3)
	defer sut.StorageManager().Close()

	testutil.CreateFile(ts, filepath.Join(path, testFile), make([]byte, 10*400))

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()

		block1 := types.Block{Filename: testFile, Number: 1}
		buf, _ := sut.Pin(block1)

		assert.NotPanics(t, func() {
			for i := 0; i < 10000; i++ {
				sut.Unpin(buf)
				buf, _ = sut.Pin(block1)
			}
		})
	}()

	go func() {
		defer wg.Done()

		block1 := types.Block{Filename: testFile, Number: 1}
		buf, _ := sut.Pin(block1)

		assert.NotPanics(t, func() {
			for i := 0; i < 10000; i++ {
				sut.Unpin(buf)
				buf, _ = sut.Pin(block1)
			}
		})
	}()

	wg.Wait()
}
