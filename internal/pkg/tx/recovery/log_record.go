// Записи в журнале восстановления

package recovery

import (
	"github.com/pkg/errors"

	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
)

const (
	int32Size = 4
	int64Size = 8
)

type trxInt interface {
	Pin(block *storage.BlockID) error
	Unpin(block *storage.BlockID) error
	SetString(block *storage.BlockID, offset uint32, value string, okToLog bool) error
	SetInt64(block *storage.BlockID, offset uint32, value int64, okToLog bool) error
}

type LogRecord interface {
	Op() uint32
	TXNum() int32
	Undo(tx trxInt) error
	MarshalBytes() []byte
}

const (
	CheckpointOp uint32 = 255
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
	case CheckpointOp:
		return NewCheckpointLogRecordFromBytes(rawRecord)
	case StartOp:
		return NewStartLogRecordFromBytes(rawRecord)
	case CommitOp:
		return NewCommitLogRecordFromBytes(rawRecord)
	case RollbackOp:
		return NewRollbackLogRecordFromBytes(rawRecord)
	case SetStringOp:
		return NewSetStringLogRecordFromBytes(rawRecord)
	case SetInt64Op:
		return NewSetInt64LogRecordFromBytes(rawRecord)
	default:
		return nil, errors.WithMessagef(ErrUnknownLogRecord, "%d is an unknown op", op)
	}
}

type BaseLogRecord struct {
	op    uint32
	txnum int32
}

func (lr *BaseLogRecord) Op() uint32 {
	return lr.op
}

func (lr *BaseLogRecord) TXNum() int32 {
	return lr.txnum
}

func (lr *BaseLogRecord) Undo(tx trxInt) error {
	return nil
}

func (lr *BaseLogRecord) MarshalBytes() []byte {
	oppos := uint32(0)
	txpos := oppos + int32Size
	recLen := txpos + int32Size

	p := storage.NewPage(recLen)

	p.SetUint32(oppos, lr.op)
	p.SetInt32(txpos, lr.txnum)

	return p.Content()
}

func (lr *BaseLogRecord) unmarshalBytes(rawRecord []byte) error {
	p := storage.NewPageFromBytes(rawRecord)

	lr.op = p.GetUint32(0)
	lr.txnum = p.GetInt32(int32Size)

	return nil
}
