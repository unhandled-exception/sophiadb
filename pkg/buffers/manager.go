package buffers

import (
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/unhandled-exception/sophiadb/pkg/storage"
	"github.com/unhandled-exception/sophiadb/pkg/types"
	"github.com/unhandled-exception/sophiadb/pkg/utils"
	"github.com/unhandled-exception/sophiadb/pkg/wal"
)

var defaultMaxPinTimeout time.Duration = 10 * time.Second

// Manager менеджер буферов в памяти
type Manager struct {
	mu sync.Mutex

	Len            int
	PinLockTimeout time.Duration

	fm *storage.Manager
	lm *wal.Manager

	pinLock   *utils.Cond
	pool      *BuffersPool
	available int
}

type ManagerOpt func(*Manager)

// NewManager создает новый менеджер пулов
func NewManager(fm *storage.Manager, lm *wal.Manager, pLen int, opts ...ManagerOpt) *Manager {
	bm := &Manager{
		fm:             fm,
		lm:             lm,
		Len:            pLen,
		available:      pLen,
		PinLockTimeout: defaultMaxPinTimeout,
		pinLock:        utils.NewCond(&sync.Mutex{}),
	}

	bm.pool = NewBuffersPool(pLen, bm.newBuffer)

	for _, opt := range opts {
		opt(bm)
	}

	return bm
}

func WithPinLockTimeout(pinLockTimeout time.Duration) ManagerOpt {
	return func(m *Manager) {
		m.PinLockTimeout = pinLockTimeout
	}
}

// StorageManager возвращает менеджер хранилища
func (bm *Manager) StorageManager() *storage.Manager {
	return bm.fm
}

func (bm *Manager) newBuffer() *Buffer {
	return NewBuffer(bm.fm, bm.lm)
}

// SetMaxPinLockTime Задает максимальное время ожидания освобождения буферов
func (bm *Manager) SetMaxPinLockTime(t time.Duration) {
	bm.PinLockTimeout = t
}

// Available возвращает число доступных буферов
func (bm *Manager) Available() int {
	return bm.available
}

// FlushAll сбрасывает все буферы транзакции на диск
func (bm *Manager) FlushAll(txnum types.TRX) error {
	return bm.pool.FlushAll(txnum)
}

// Unpin уменьшает счетчик закреплений. Если буфер освободился, то дает сигнал другим потокам, что появился свободный буфер
func (bm *Manager) Unpin(buf *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	buf.Unpin()

	if !buf.IsPinned() {
		bm.available++
		bm.pinLock.Broadcast()
	}
}

// Pin — закрепляет блок в памяти
func (bm *Manager) Pin(block types.Block) (*Buffer, error) {
	buf, err := bm.tryToPin(block)
	if err != nil && !errors.Is(err, ErrNoAvailableBuffers) {
		return nil, err
	}

	if errors.Is(err, ErrNoAvailableBuffers) {
		bm.pinLock.L.Lock()
		defer bm.pinLock.L.Unlock()

		deadline := time.Now().Add(bm.PinLockTimeout)

		for buf == nil && time.Now().Before(deadline) {
			bm.pinLock.WaitWithTimeout(bm.PinLockTimeout)

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

func (bm *Manager) tryToPin(block types.Block) (*Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

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
