// Записи в журнале восстановления

package recovery

import (
	"github.com/pkg/errors"

	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

const (
	int32Size = 4
	int64Size = 8
)

type LogRecord interface {
	String() string
	Op() uint32
	TXNum() types.TRX
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

func NewLogRecordFromBytes(rawRecord []byte) (LogRecord, error) { //nolint:ireturn
	if len(rawRecord) == 0 {
		return nil, ErrEmptyLogRecord
	}

	p := types.NewPageFromBytes(rawRecord)
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
	txnum types.TRX
}

func (lr BaseLogRecord) String() string {
	return ""
}

func (lr BaseLogRecord) Op() uint32 {
	return lr.op
}

func (lr BaseLogRecord) TXNum() types.TRX {
	return lr.txnum
}

func (lr BaseLogRecord) Undo(tx trxInt) error {
	return nil
}

func (lr BaseLogRecord) MarshalBytes() []byte {
	oppos := uint32(0)
	txpos := oppos + int32Size
	recLen := txpos + int32Size

	p := types.NewPage(recLen)

	p.SetUint32(oppos, lr.op)
	p.SetInt32(txpos, int32(lr.txnum))

	return p.Content()
}

func (lr *BaseLogRecord) unmarshalBytes(rawRecord []byte) error {
	p := types.NewPageFromBytes(rawRecord)

	lr.op = p.GetUint32(0)
	lr.txnum = types.TRX(p.GetInt32(int32Size))

	return nil
}
