package concurrency

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/pkg/types"
	"github.com/unhandled-exception/sophiadb/pkg/utils"
)

var defaultMaxLockWaitTime time.Duration = 10 * time.Second

const XLockValue int32 = -1

type LockTable struct {
	locks map[types.Block]int32

	L               sync.RWMutex
	locksCond       *utils.Cond
	lockWaitTimeout time.Duration
}

type lockTableOpt func(lt *LockTable)

func NewLockTable(opts ...lockTableOpt) *LockTable {
	lt := &LockTable{
		locks: make(map[types.Block]int32),

		locksCond:       utils.NewCond(&sync.Mutex{}),
		lockWaitTimeout: defaultMaxLockWaitTime,
	}

	for _, opt := range opts {
		opt(lt)
	}

	return lt
}

func WithLockWaitTimeout(timeout time.Duration) lockTableOpt {
	return func(lt *LockTable) {
		if timeout != 0 {
			lt.lockWaitTimeout = timeout
		}
	}
}

func (lt *LockTable) LocksCount(block types.Block) int32 {
	lCount := lt.locks[block]

	return lCount
}

func (lt *LockTable) HasXLock(block types.Block) bool {
	return lt.LocksCount(block) == XLockValue
}

func (lt *LockTable) HasOtherSLock(block types.Block) bool {
	// Менеджер конкуренции берет slock перед xlock, поэтому единица равна ровно одной блокировке и можно брать xlock
	return lt.LocksCount(block) > 1
}

// SLock устанавливает разделеяемую блокировку для блока (shared lock)
func (lt *LockTable) SLock(block types.Block) error {
	deadline := time.Now().Add(lt.lockWaitTimeout)

	lt.locksCond.L.Lock()
	defer lt.locksCond.L.Unlock()

	var err error

	for {
		lt.L.Lock()
		err = lt.tryToSLock(block)
		lt.L.Unlock()

		if err == nil {
			break
		}

		if !time.Now().Before(deadline) {
			break
		}

		lt.locksCond.WaitWithTimeout(lt.lockWaitTimeout)
	}

	return err
}

func (lt *LockTable) tryToSLock(block types.Block) error {
	if lt.HasXLock(block) {
		return errors.WithMessagef(ErrLockAbort, "slock: block %s has xlock", block)
	}

	lCount := lt.locks[block]
	lt.locks[block] = lCount + 1

	return nil
}

// XLock устанавливает эксклюзивную блокировку для блока (shared lock)
func (lt *LockTable) XLock(block types.Block) error {
	deadline := time.Now().Add(lt.lockWaitTimeout)

	lt.locksCond.L.Lock()
	defer lt.locksCond.L.Unlock()

	var err error

	for {
		lt.L.Lock()
		err = lt.tryToXLock(block)
		lt.L.Unlock()

		if err == nil {
			break
		}

		if !time.Now().Before(deadline) {
			break
		}

		lt.locksCond.WaitWithTimeout(lt.lockWaitTimeout)
	}

	return err
}

func (lt *LockTable) tryToXLock(block types.Block) error {
	if lt.HasOtherSLock(block) {
		return errors.WithMessagef(ErrLockAbort, "xlock: block %s has other %d slock", block, lt.LocksCount(block))
	}

	lt.locks[block] = XLockValue

	return nil
}

// Unlock снимает блокировку для блока
func (lt *LockTable) Unlock(block types.Block) {
	lt.L.Lock()

	if lCount := lt.locks[block]; lCount > 1 {
		lt.locks[block] = lCount - 1
	} else {
		delete(lt.locks, block)
	}

	lt.L.Unlock()

	lt.locksCond.Broadcast()
}
