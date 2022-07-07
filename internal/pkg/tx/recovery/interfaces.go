package recovery

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

type RecoveryManager interface {
	Commit() error
	Rollback() error
	Recover() error
	SetInt64(buf buffer, offset uint32, value int64) (types.LSN, error)
	SetString(buf buffer, offset uint32, value string) (types.LSN, error)
}

type trxInt interface {
	Pin(block *types.Block) error
	Unpin(block *types.Block)
	SetString(block *types.Block, offset uint32, value string, okToLog bool) error
	SetInt64(block *types.Block, offset uint32, value int64, okToLog bool) error
	TXNum() types.TRX
}

type BuffersManager interface {
	FlushAll(txnum types.TRX) error
}

type WALManager interface {
	Flush(lsn types.LSN, force bool) error
	Append(logRec []byte) (types.LSN, error)
	Iterator() (*wal.Iterator, error)
}

type buffer interface {
	Content() *types.Page
	Block() *types.Block
}
