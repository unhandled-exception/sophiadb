package transaction

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/concurrency"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type walManager interface {
	recovery.WALManager
}

type storageManager interface {
	BlockSize() uint32
	Length(filename string) (int32, error)
	Append(filename string) (*types.Block, error)
}

type buffersManager interface {
	recovery.BuffersManager
	Pin(block *types.Block) (*buffers.Buffer, error)
	Unpin(buf *buffers.Buffer)
	Available() int
}

type concurrencyManager interface {
	concurrency.ConcurrencyManager
}

type recoveryManager interface {
	recovery.RecoveryManager
}

type bufferList interface {
	GetBuffer(block *types.Block) *buffers.Buffer
	Pin(block *types.Block) error
	Unpin(block *types.Block)
	UnpinAll()
}
