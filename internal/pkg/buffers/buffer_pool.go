package buffers

import (
	"container/ring"
	"sync"

	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
)

type buffersPool struct {
	sync.Mutex

	len             int
	ring            *ring.Ring         // Буферы храним в кольце, чтобы реализовать круговую стратегию поиска свобоных буферов
	blocksToBuffers map[string]*Buffer // Для ускорения поиска блоков используем словарь с ключем BlockID.HashKey()
}

type newBufferFunc func() *Buffer

// newBuffersPool создает новый пул буферов
func newBuffersPool(len int, nbf newBufferFunc) *buffersPool {
	bp := &buffersPool{
		len:             len,
		blocksToBuffers: make(map[string]*Buffer, len),
		ring:            ring.New(len),
	}
	for i := 0; i < len; i++ {
		bp.ring.Value = nbf()
		bp.ring = bp.ring.Next()
	}
	return bp
}

// buffers возвращает массив буферов в виде слайса
func (bp *buffersPool) buffers() []*Buffer {
	buffers := make([]*Buffer, bp.len)
	for i := 0; i < bp.len; i++ {
		buffers[i] = bp.ring.Value.(*Buffer)
		bp.ring = bp.ring.Next()
	}
	return buffers
}

// FlushAll сбрасывает на диск все блоки, соответствующие транзакции
func (bp *buffersPool) FlushAll(txnum int64) error {
	bp.Lock()
	defer bp.Unlock()

	for i := 0; i < bp.len; i++ {
		buf := bp.ring.Value.(*Buffer)
		if buf.ModifyingTX() == txnum {
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
func (bp *buffersPool) FindExistingBuffer(block *storage.BlockID) *Buffer {
	if buf, ok := bp.blocksToBuffers[block.HashKey()]; ok {
		return buf
	}
	return nil
}

// ChooseUnpinnedBuffer ищет незакрепленные буферы в памяти
func (bp *buffersPool) ChooseUnpinnedBuffer() *Buffer {
	for i := 0; i < bp.len; i++ {
		bp.ring = bp.ring.Next()
		buf := bp.ring.Value.(*Buffer)
		if !buf.IsPinned() {
			return buf
		}
	}
	return nil
}

// AssignBufferToBlock связывает буфер с блоком на диске
func (bp *buffersPool) AssignBufferToBlock(buf *Buffer, block *storage.BlockID) error {
	bp.blocksToBuffers[block.HashKey()] = buf
	if oldBlock := buf.Block(); oldBlock != nil {
		delete(bp.blocksToBuffers, buf.Block().HashKey())
	}
	return buf.AssignToBlock(block)
}
