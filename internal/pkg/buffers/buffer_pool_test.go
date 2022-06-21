package buffers

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/test"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

func TestBufferPoolTestSuite(t *testing.T) {
	suite.Run(t, new(BuffersPoolTestSuite))
}

type BuffersPoolTestSuite struct {
	suite.Suite
	suiteDir string
}

func (ts *BuffersPoolTestSuite) SuiteDir() string {
	return ts.suiteDir
}

func (ts *BuffersPoolTestSuite) SetupSuite() {
	testSuiteDir := "buffers_pool_tests"
	ts.suiteDir = test.CreateSuiteTemporaryDir(ts, testSuiteDir)
}

func (ts *BuffersPoolTestSuite) TearDownSuite() {
	test.RemoveSuiteTemporaryDir(ts)
}

func (ts *BuffersPoolTestSuite) createBufferPool(bLen int) (*buffersPool, string, *storage.Manager) {
	var defaultBlockSize uint32 = 400

	walFile := "wal_log.dat"

	path := test.CreateTestTemporaryDir(ts)
	fm, err := storage.NewFileManager(path, defaultBlockSize)
	ts.Require().NoError(err)

	lm, err := wal.NewManager(fm, walFile)
	ts.Require().NoError(err)

	ts.Require().FileExists(filepath.Join(path, walFile))

	bp := newBuffersPool(bLen, func() *Buffer {
		return NewBuffer(fm, lm)
	})

	return bp, path, fm
}

func (ts *BuffersPoolTestSuite) TestFindExistingBuffer() {
	bp, path, fm := ts.createBufferPool(10)
	defer fm.Close()

	defaultBlockSize := 400
	testFile := "test_file_1.dat"
	test.CreateFile(ts, filepath.Join(path, testFile), make([]byte, bp.len*defaultBlockSize))

	buffers := bp.buffers()

	block0 := storage.NewBlockID(testFile, 0)
	block1 := storage.NewBlockID(testFile, 1)
	block2 := storage.NewBlockID(testFile, 2)
	block3 := storage.NewBlockID(testFile, 3)

	ts.Require().NoError(bp.AssignBufferToBlock(buffers[0], block0))
	ts.Require().NoError(bp.AssignBufferToBlock(buffers[1], block1))

	ts.NotNil(bp.FindExistingBuffer(block0))
	ts.NotNil(bp.FindExistingBuffer(block1))
	ts.Nil(bp.FindExistingBuffer(block2))
	ts.Nil(bp.FindExistingBuffer(block3))

	ts.Require().NoError(bp.AssignBufferToBlock(buffers[0], block2))
	ts.Nil(bp.FindExistingBuffer(block0))
	ts.NotNil(bp.FindExistingBuffer(block2))
}

func (ts *BuffersPoolTestSuite) TestChooseUnpinnedBuffer() {
	bp, path, fm := ts.createBufferPool(5)
	defer fm.Close()

	defaultBlockSize := 400
	testFile := "test_file_2.dat"
	test.CreateFile(ts, filepath.Join(path, testFile), make([]byte, bp.len*defaultBlockSize))

	var buf *Buffer

	for i := 0; i < bp.len; i++ {
		if i%2 == 0 {
			v, ok := bp.ring.Value.(*Buffer)
			ts.Require().True(ok, "failed to cast ring value")

			v.Pin()
		}

		bp.ring = bp.ring.Next()
	}

	var pins [5]bool

	for i := 0; i < bp.len; i++ {
		v, ok := bp.ring.Value.(*Buffer)
		ts.Require().True(ok, "failed to cast ring value")

		pins[i] = v.IsPinned()
		bp.ring = bp.ring.Next()
	}

	ts.Require().Equal([5]bool{true, false, true, false, true}, pins)

	buf = bp.ChooseUnpinnedBuffer()
	ts.Require().NotNil(buf)

	ts.Require().False(buf.IsPinned())

	buf.Pin()

	buf2 := bp.ChooseUnpinnedBuffer()
	ts.Require().NotNil(buf2)

	ts.Require().False(buf2.IsPinned())

	buf2.Pin()

	ts.Require().Nil(bp.ChooseUnpinnedBuffer())

	buf2.Unpin()
	ts.Require().NotNil(bp.ChooseUnpinnedBuffer())
}
