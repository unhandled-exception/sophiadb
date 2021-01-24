package buffers

import (
	"sync"

	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
)

type buffersPool struct {
	sync.Mutex

	len     int
	buffers []*Buffer
}

type newBufferFunc func() *Buffer

// newBuffersPool создает новый пул буферов
func newBuffersPool(len int, nbf newBufferFunc) *buffersPool {
	bp := &buffersPool{
		len:     len,
		buffers: make([]*Buffer, len),
	}
	var i int
	for i = 0; i < len; i++ {
		bp.buffers[i] = nbf()
	}
	return bp
}

// FlushAll сбрасывает на диск все блоки, соответствующие транзакции
func (bp *buffersPool) FlushAll(txnum int64) error {
	bp.Lock()
	defer bp.Unlock()

	for _, buf := range bp.buffers {
		if buf.ModifyingTX() == txnum {
			err := buf.Flush()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// FindExistingBuffer ищет существующий буфер, соотоветсвующий блоку
func (bp *buffersPool) FindExistingBuffer(block *storage.BlockID) *Buffer {
	for _, buf := range bp.buffers {
		b := buf.Block()
		if b != nil && b.Equals(block) {
			return buf
		}
	}
	return nil
}

// ChooseUnpinnedBuffer ищет незакрепленные буферы в памяти
func (bp *buffersPool) ChooseUnpinnedBuffer() *Buffer {
	for _, buf := range bp.buffers {
		if !buf.IsPinned() {
			return buf
		}
	}
	return nil
}

// AssignBufferToBlock связывает буфер с блоком на диске
// Заглушка, чтобы подновлять структуры для поиска, когда они появятся
func (bp *buffersPool) AssignBufferToBlock(buf *Buffer, block *storage.BlockID) error {
	return buf.AssignToBlock(block)
}
