package recovery

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
)

type CheckpointLogRecord struct {
	BaseLogRecord
}

func NewCheckpointLogRecord() *CheckpointLogRecord {
	return &CheckpointLogRecord{
		BaseLogRecord: BaseLogRecord{
			op: CheckpointOp,
		},
	}
}

func NewCheckpointLogRecordFromBytes(rawRecord []byte) (*CheckpointLogRecord, error) {
	r := CheckpointLogRecord{}

	err := r.unmarshalBytes(rawRecord)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (lr *CheckpointLogRecord) String() string {
	return `<CHECKPOINT>`
}

func (lr *CheckpointLogRecord) MarshalBytes() []byte {
	oppos := uint32(0)
	recLen := oppos + int32Size

	p := storage.NewPage(recLen)

	p.SetUint32(oppos, lr.op)

	return p.Content()
}

func (lr *CheckpointLogRecord) unmarshalBytes(rawRecord []byte) error {
	p := storage.NewPageFromBytes(rawRecord)

	lr.op = p.GetUint32(0)

	return nil
}
