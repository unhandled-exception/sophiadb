package buffers

import (
	"container/ring"
	"sync"

	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
)

type BuffersPool struct {
	sync.Mutex

	len             int
	ring            *ring.Ring         // Буферы храним в кольце, чтобы реализовать круговую стратегию поиска свобоных буферов
	blocksToBuffers map[string]*Buffer // Для ускорения поиска блоков используем словарь с ключем BlockID.HashKey()
}

type newBufferFunc func() *Buffer

// NewBuffersPool создает новый пул буферов
func NewBuffersPool(bLen int, nbf newBufferFunc) *BuffersPool {
	bp := &BuffersPool{
		len:             bLen,
		blocksToBuffers: make(map[string]*Buffer, bLen),
		ring:            ring.New(bLen),
	}

	for i := 0; i < bLen; i++ {
		bp.ring.Value = nbf()
		bp.ring = bp.ring.Next()
	}

	return bp
}

// Buffers возвращает массив буферов в виде слайса
func (bp *BuffersPool) Buffers() []*Buffer {
	buffers := make([]*Buffer, bp.len)

	for i := 0; i < bp.len; i++ {
		buffers[i], _ = bp.ring.Value.(*Buffer)
		bp.ring = bp.ring.Next()
	}

	return buffers
}

// FlushAll сбрасывает на диск все блоки, соответствующие транзакции
func (bp *BuffersPool) FlushAll(txnum int64) error {
	bp.Lock()
	defer bp.Unlock()

	for i := 0; i < bp.len; i++ {
		buf, ok := bp.ring.Value.(*Buffer)
		if ok && buf.ModifyingTX() == txnum {
			err := buf.Flush()
			if err != nil {
				return err
			}
		}

		bp.ring = bp.ring.Next()
	}

	return nil
}

// FindExistingBuffer ищет существующий буфер, соотоветсвующий блоку
func (bp *BuffersPool) FindExistingBuffer(block *storage.BlockID) *Buffer {
	if buf, ok := bp.blocksToBuffers[block.HashKey()]; ok {
		return buf
	}

	return nil
}

// ChooseUnpinnedBuffer ищет незакрепленные буферы в памяти
func (bp *BuffersPool) ChooseUnpinnedBuffer() *Buffer {
	for i := 0; i < bp.len; i++ {
		bp.ring = bp.ring.Next()

		buf, ok := bp.ring.Value.(*Buffer)
		if ok && !buf.IsPinned() {
			return buf
		}
	}

	return nil
}

// AssignBufferToBlock связывает буфер с блоком на диске
func (bp *BuffersPool) AssignBufferToBlock(buf *Buffer, block *storage.BlockID) error {
	bp.blocksToBuffers[block.HashKey()] = buf

	if oldBlock := buf.Block(); oldBlock != nil {
		delete(bp.blocksToBuffers, buf.Block().HashKey())
	}

	return buf.AssignToBlock(block)
}
