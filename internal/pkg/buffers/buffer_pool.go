package buffers

import (
	"container/ring"
	"sync"

	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type BuffersPool struct {
	mu sync.Mutex

	ring            *ring.Ring // Буферы храним в кольце, чтобы реализовать круговую стратегию поиска свобоных буферов
	blocksToBuffers map[types.Block]*Buffer
	len             int
}

type newBufferFunc func() *Buffer

// NewBuffersPool создает новый пул буферов
func NewBuffersPool(bLen int, nbf newBufferFunc) *BuffersPool {
	bp := &BuffersPool{
		len:             bLen,
		blocksToBuffers: make(map[types.Block]*Buffer, bLen),
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
	bp.mu.Lock()
	defer bp.mu.Unlock()

	buffers := make([]*Buffer, bp.len)

	for i := 0; i < bp.len; i++ {
		buffers[i], _ = bp.ring.Value.(*Buffer)
		bp.ring = bp.ring.Next()
	}

	return buffers
}

// FlushAll сбрасывает на диск все блоки, соответствующие транзакции
func (bp *BuffersPool) FlushAll(txnum types.TRX) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

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
func (bp *BuffersPool) FindExistingBuffer(block types.Block) *Buffer {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if buf, ok := bp.blocksToBuffers[block]; ok {
		return buf
	}

	return nil
}

// ChooseUnpinnedBuffer ищет незакрепленные буферы в памяти
func (bp *BuffersPool) ChooseUnpinnedBuffer() *Buffer {
	bp.mu.Lock()
	defer bp.mu.Unlock()

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
func (bp *BuffersPool) AssignBufferToBlock(buf *Buffer, block types.Block) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.blocksToBuffers[block] = buf

	delete(bp.blocksToBuffers, buf.Block())

	return buf.AssignToBlock(block)
}
