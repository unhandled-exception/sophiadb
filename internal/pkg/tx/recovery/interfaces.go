package recovery

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

type trxInt interface {
	Pin(block *types.BlockID) error
	Unpin(block *types.BlockID) error
	SetString(block *types.BlockID, offset uint32, value string, okToLog bool) error
	SetInt64(block *types.BlockID, offset uint32, value int64, okToLog bool) error
	TXNum() types.TRX
}

type bufferManager interface {
	FlushAll(txnum types.TRX) error
}

type walManager interface {
	Flush(lsn types.LSN, force bool) error
	Append(logRec []byte) (types.LSN, error)
	Iterator() (*wal.Iterator, error)
}

type buffer interface {
	Content() *types.Page
	Block() *types.BlockID
}
