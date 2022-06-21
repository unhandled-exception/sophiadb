package buffers

import (
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/utils"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

var defaultMaxPinTime time.Duration = 10 * time.Second

// ErrNoAvailableBuffers — нет свободных буферов в памяти
var ErrNoAvailableBuffers = errors.New("no available buffers")

// Manager менеджер буферов в памяти
type Manager struct {
	sync.Mutex
	pinLock *utils.Cond

	fm *storage.Manager
	lm *wal.Manager

	pool           *buffersPool
	len            int
	available      int
	maxPinLockTime time.Duration
}

// NewManager создает новый менеджер пулов
func NewManager(fm *storage.Manager, lm *wal.Manager, pLen int) *Manager {
	bm := &Manager{
		fm:             fm,
		lm:             lm,
		len:            pLen,
		available:      pLen,
		maxPinLockTime: defaultMaxPinTime,
		pinLock:        utils.NewCond(&sync.Mutex{}),
	}
	bm.pool = newBuffersPool(pLen, bm.newBuffer)

	return bm
}

func (bm *Manager) newBuffer() *Buffer {
	return NewBuffer(bm.fm, bm.lm)
}

// SetMaxPinLockTime Задает максимальное время ожидания освобождения буферов
func (bm *Manager) SetMaxPinLockTime(t time.Duration) {
	bm.maxPinLockTime = t
}

// Available возвращает число доступных буферов
func (bm *Manager) Available() int {
	return bm.available
}

// FlushAll сбрасывает все буферы транзакции на диск
func (bm *Manager) FlushAll(txnum int64) error {
	return bm.pool.FlushAll(txnum)
}

// Unpin уменьшает счетчик закреплений. Если буфер освободился, то дает сигнал другим потокам, что появился свободный буфер
func (bm *Manager) Unpin(buf *Buffer) {
	bm.Lock()
	defer bm.Unlock()

	buf.Unpin()

	if !buf.IsPinned() {
		bm.available++
		bm.pinLock.Broadcast()
	}
}

// Pin — закрепляет блок в памяти
func (bm *Manager) Pin(block *storage.BlockID) (*Buffer, error) {
	buf, err := bm.tryToPin(block)
	if err != nil && !errors.Is(err, ErrNoAvailableBuffers) {
		return nil, err
	}

	if buf != nil {
		bm.pinLock.L.Lock()
		defer bm.pinLock.L.Unlock()

		deadline := time.Now().Add(bm.maxPinLockTime)

		for buf == nil && time.Now().Before(deadline) {
			bm.pinLock.WaitWithTimeout(bm.maxPinLockTime)

			buf, err = bm.tryToPin(block)
			if err != nil && !errors.Is(err, ErrNoAvailableBuffers) {
				return nil, err
			}
		}
	}

	if buf == nil {
		return nil, ErrNoAvailableBuffers
	}

	return buf, nil
}

func (bm *Manager) tryToPin(block *storage.BlockID) (*Buffer, error) {
	buf := bm.pool.FindExistingBuffer(block)
	if buf == nil {
		buf = bm.pool.ChooseUnpinnedBuffer()
		if buf == nil {
			return nil, ErrNoAvailableBuffers
		}

		err := bm.pool.AssignBufferToBlock(buf, block)
		if err != nil {
			return nil, err
		}
	}

	if !buf.IsPinned() {
		bm.available--
	}

	buf.Pin()

	return buf, nil
}
