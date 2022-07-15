package buffers_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
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
	ts.suiteDir = testutil.CreateSuiteTemporaryDir(ts, testSuiteDir)
}

func (ts *BuffersPoolTestSuite) TearDownSuite() {
	testutil.RemoveSuiteTemporaryDir(ts)
}

func (ts *BuffersPoolTestSuite) createBufferPool(bLen int) (*buffers.BuffersPool, string, *storage.Manager) {
	var defaultBlockSize uint32 = 400

	walFile := "wal_log.dat"

	path := testutil.CreateTestTemporaryDir(ts)
	fm, err := storage.NewFileManager(path, defaultBlockSize)
	ts.Require().NoError(err)

	lm, err := wal.NewManager(fm, walFile)
	ts.Require().NoError(err)

	ts.Require().FileExists(filepath.Join(path, walFile))

	bp := buffers.NewBuffersPool(bLen, func() *buffers.Buffer {
		return buffers.NewBuffer(fm, lm)
	})

	return bp, path, fm
}

func (ts *BuffersPoolTestSuite) TestFindExistingBuffer() {
	bpLen := 10
	bp, path, fm := ts.createBufferPool(bpLen)

	defer fm.Close()

	defaultBlockSize := 400
	testFile := "test_file_1.dat"
	testutil.CreateFile(ts, filepath.Join(path, testFile), make([]byte, bpLen*defaultBlockSize))

	buffers := bp.Buffers()

	block0 := types.Block{Filename: testFile, Number: 0}
	block1 := types.Block{Filename: testFile, Number: 1}
	block2 := types.Block{Filename: testFile, Number: 2}
	block3 := types.Block{Filename: testFile, Number: 3}

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
	bpLen := 5
	bp, path, fm := ts.createBufferPool(5)

	defer fm.Close()

	defaultBlockSize := 400
	testFile := "test_file_2.dat"
	testutil.CreateFile(ts, filepath.Join(path, testFile), make([]byte, bpLen*defaultBlockSize))

	pins := make([]bool, bpLen)

	for i, buf := range bp.Buffers() {
		if i%2 == 0 {
			buf.Pin()
		}

		pins[i] = buf.IsPinned()
	}

	ts.Require().Equal([]bool{true, false, true, false, true}, pins)

	buf := bp.ChooseUnpinnedBuffer()
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
