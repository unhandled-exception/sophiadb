package concurrency_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/tx/concurrency"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

type ConcurrencyManagerTestSute struct {
	suite.Suite
}

func TestConcurrencyManagerTestSute(t *testing.T) {
	suite.Run(t, new(ConcurrencyManagerTestSute))
}

func (ts *ConcurrencyManagerTestSute) newManager() (*concurrency.Manager, *concurrency.LockTable) {
	lt := concurrency.NewLockTable(
		concurrency.WithLockWaitTimeout(10 * time.Millisecond),
	)

	return concurrency.NewManager(lt), lt
}

func (ts *ConcurrencyManagerTestSute) TestSLock_OK() {
	t := ts.T()

	sut, _ := ts.newManager()

	block1 := types.Block{Filename: testBlockFilename, Number: 1}

	assert.NoError(t, sut.SLock(block1))
	assert.True(t, sut.HasSlock(block1))
}

func (ts *ConcurrencyManagerTestSute) TestSLock_Fail() {
	t := ts.T()

	sut, lt := ts.newManager()

	block1 := types.Block{Filename: testBlockFilename, Number: 1}
	_ = lt.XLock(block1)

	assert.ErrorIs(t, sut.SLock(block1), concurrency.ErrLockAbort)
	assert.False(t, sut.HasSlock(block1))
}

func (ts *ConcurrencyManagerTestSute) TestXLock_OK() {
	t := ts.T()

	sut, _ := ts.newManager()

	block1 := types.Block{Filename: testBlockFilename, Number: 1}

	assert.NoError(t, sut.XLock(block1))
	assert.True(t, sut.HasXlock(block1))

	assert.NoError(t, sut.XLock(block1))
	assert.NoError(t, sut.SLock(block1))
}

func (ts *ConcurrencyManagerTestSute) TestXLock_Fail() {
	t := ts.T()

	sut, lt := ts.newManager()

	block1 := types.Block{Filename: testBlockFilename, Number: 1}

	_ = lt.XLock(block1)
	assert.ErrorIs(t, sut.XLock(block1), concurrency.ErrLockAbort)

	lt.Unlock(block1)
	_ = lt.SLock(block1)
	assert.ErrorIs(t, sut.XLock(block1), concurrency.ErrLockAbort)
	assert.False(t, sut.HasXlock(block1))
}

func (ts *ConcurrencyManagerTestSute) TestRelease() {
	t := ts.T()

	sut, _ := ts.newManager()

	block1 := types.Block{Filename: testBlockFilename, Number: 1}
	block2 := types.Block{Filename: testBlockFilename, Number: 2}

	assert.NoError(t, sut.XLock(block1))
	assert.NoError(t, sut.SLock(block2))

	sut.Release()

	assert.False(t, sut.HasXlock(block1))
	assert.False(t, sut.HasSlock(block2))
}
