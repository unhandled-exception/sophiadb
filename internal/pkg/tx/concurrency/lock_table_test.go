package concurrency_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/concurrency"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

const testBlockFilename = "lock_table"

type LockTableTestSuite struct {
	suite.Suite
}

func TestLockTableTestSuite(t *testing.T) {
	suite.Run(t, new(LockTableTestSuite))
}

func (ts *LockTableTestSuite) TestSlock_OK() {
	t := ts.T()

	sut := concurrency.NewLockTable()

	block1 := types.NewBlock(testBlockFilename, 1)
	block2 := types.NewBlock(testBlockFilename, 2)

	assert.NoError(t, sut.SLock(block1))
	assert.NoError(t, sut.SLock(block2))
	assert.NoError(t, sut.SLock(block1))
	assert.NoError(t, sut.SLock(block2))

	assert.EqualValues(t, 2, sut.LocksCount(block1))
	assert.EqualValues(t, 2, sut.LocksCount(block2))
}

func (ts *LockTableTestSuite) TestXLock_OK() {
	t := ts.T()

	sut := concurrency.NewLockTable(
		concurrency.WithLockWaitTimeout(100 * time.Millisecond),
	)

	block1 := types.NewBlock(testBlockFilename, 1)

	assert.NoError(t, sut.SLock(block1))
	assert.NoError(t, sut.XLock(block1))
	assert.NoError(t, sut.XLock(block1))

	assert.ErrorIs(t, sut.SLock(block1), concurrency.ErrLockAbort)
}

func (ts *LockTableTestSuite) TestSLock_FailedToLockIfHasXLock() {
	t := ts.T()

	sut := concurrency.NewLockTable(
		concurrency.WithLockWaitTimeout(100 * time.Millisecond),
	)

	block1 := types.NewBlock(testBlockFilename, 1)
	assert.NoError(t, sut.XLock(block1))
	assert.ErrorIs(t, sut.SLock(block1), concurrency.ErrLockAbort)
}

func (ts *LockTableTestSuite) TestXLock_FailedToLockIfOtherHasSLock() {
	t := ts.T()

	sut := concurrency.NewLockTable(
		concurrency.WithLockWaitTimeout(100 * time.Millisecond),
	)

	block1 := types.NewBlock(testBlockFilename, 1)

	assert.NoError(t, sut.SLock(block1))
	assert.NoError(t, sut.SLock(block1))
	assert.ErrorIs(t, sut.XLock(block1), concurrency.ErrLockAbort)
}

func (ts *LockTableTestSuite) TestUnlock() {
	t := ts.T()

	sut := concurrency.NewLockTable(
		concurrency.WithLockWaitTimeout(100 * time.Millisecond),
	)

	block1 := types.NewBlock(testBlockFilename, 1)
	block2 := types.NewBlock(testBlockFilename, 2)

	assert.NoError(t, sut.SLock(block1))
	assert.NoError(t, sut.SLock(block1))
	sut.Unlock(block1)

	assert.NoError(t, sut.XLock(block1))
	assert.NoError(t, sut.XLock(block1))
	sut.Unlock(block1)

	assert.NoError(t, sut.SLock(block1))

	sut.Unlock(block2)
	assert.NoError(t, sut.SLock(block2))
}

func (ts *LockTableTestSuite) TestLocksConcurrently_OK() {
	t := ts.T()

	sut := concurrency.NewLockTable(
		concurrency.WithLockWaitTimeout(1 * time.Millisecond),
	)

	block1 := types.NewBlock(testBlockFilename, 1)
	block2 := types.NewBlock(testBlockFilename, 2)

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()

		assert.NotPanics(t, func() {
			for i := 0; i < 100; i++ {
				sut.Unlock(block2)
				_ = sut.SLock(block1)
				_ = sut.XLock(block2)
			}
		})
	}()

	go func() {
		defer wg.Done()

		assert.NotPanics(t, func() {
			for i := 0; i < 100; i++ {
				sut.Unlock(block1)
				_ = sut.SLock(block2)
				_ = sut.XLock(block1)
			}
		})
	}()

	wg.Wait()
}
