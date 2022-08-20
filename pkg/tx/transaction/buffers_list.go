package transaction

import (
	"github.com/unhandled-exception/sophiadb/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

type BufferList struct {
	bm      buffersManager
	buffers map[types.Block]*buffers.Buffer
	pins    map[types.Block]int
}

func NewBuffersList(bm buffersManager) *BufferList {
	return &BufferList{
		bm:      bm,
		buffers: make(map[types.Block]*buffers.Buffer),
		pins:    make(map[types.Block]int),
	}
}

func (bl *BufferList) GetBuffer(block types.Block) *buffers.Buffer {
	buf := bl.buffers[block]

	return buf
}

func (bl *BufferList) Pin(block types.Block) error {
	buf, err := bl.bm.Pin(block)
	if err != nil {
		return err
	}

	bl.buffers[block] = buf

	bl.pins[block] = bl.pins[block] + 1

	return nil
}

func (bl *BufferList) Unpin(block types.Block) {
	buf := bl.buffers[block]
	if buf == nil {
		return
	}

	bl.bm.Unpin(buf)

	bl.pins[block] = bl.pins[block] - 1
	if bl.pins[block] <= 0 {
		delete(bl.buffers, block)
	}
}

func (bl *BufferList) UnpinAll() {
	for block, cnt := range bl.pins {
		buf := bl.buffers[block]

		for i := 0; i < cnt; i++ {
			bl.bm.Unpin(buf)
		}
	}

	bl.buffers = make(map[types.Block]*buffers.Buffer)
	bl.pins = make(map[types.Block]int)
}
