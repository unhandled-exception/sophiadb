// Записи в журнале восстановления

package recovery

import (
	"github.com/pkg/errors"

	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
)

const (
	int32Size = 4
)

type trxInt interface {
	Pin(block *storage.BlockID) error
	Unpin(block *storage.BlockID) error
	SetString(block *storage.BlockID, offset uint32, value string, okToLog bool) error
}

type LogRecord interface {
	Op() uint32
	TXNum() int32
	Undo(tx trxInt) error
	MarshalBytes() []byte
}

const (
	CheckpointOp uint32 = 0
	StartOp      uint32 = 1
	CommitOp     uint32 = 2
	RollbackOp   uint32 = 3
	SetInt64Op   uint32 = 4
	SetStringOp  uint32 = 5
)

func NewLogRecordFromBytes(rawRecord []byte) (interface{}, error) {
	if len(rawRecord) == 0 {
		return nil, ErrEmptyLogRecord
	}

	p := storage.NewPageFromBytes(rawRecord)
	op := p.GetUint32(0)

	switch op {
	case SetStringOp:
		return NewSetStringLogRecordFromBytes(rawRecord)
	default:
		return nil, errors.WithMessagef(ErrUnknownLogRecord, "%d is an unknown op", op)
	}
}

type BaseLogRecord struct {
	op    uint32
	txnum int32
}

func (blr BaseLogRecord) Op() uint32 {
	return blr.op
}

func (blr BaseLogRecord) TXNum() int32 {
	return blr.txnum
}
