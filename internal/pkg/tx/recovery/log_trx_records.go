package recovery

import (
	"fmt"
)

type StartLogRecord struct {
	BaseLogRecord
}

func NewStartLogRecord(txnum int32) *StartLogRecord {
	return &StartLogRecord{
		BaseLogRecord: BaseLogRecord{
			op:    StartOp,
			txnum: txnum,
		},
	}
}

func NewStartLogRecordFromBytes(rawRecord []byte) (*StartLogRecord, error) {
	r := StartLogRecord{}

	err := r.unmarshalBytes(rawRecord)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (lr *StartLogRecord) String() string {
	return fmt.Sprintf(`<START, %d>`, lr.TXNum())
}

type CommitLogRecord struct {
	BaseLogRecord
}

func NewCommitLogRecord(txnum int32) *CommitLogRecord {
	return &CommitLogRecord{
		BaseLogRecord: BaseLogRecord{
			op:    CommitOp,
			txnum: txnum,
		},
	}
}

func NewCommitLogRecordFromBytes(rawRecord []byte) (*CommitLogRecord, error) {
	r := CommitLogRecord{}

	err := r.unmarshalBytes(rawRecord)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (lr *CommitLogRecord) String() string {
	return fmt.Sprintf(`<COMMIT, %d>`, lr.TXNum())
}

type RollbackLogRecord struct {
	BaseLogRecord
}

func NewRollbackLogRecord(txnum int32) *RollbackLogRecord {
	return &RollbackLogRecord{
		BaseLogRecord: BaseLogRecord{
			op:    RollbackOp,
			txnum: txnum,
		},
	}
}

func NewRollbackLogRecordFromBytes(rawRecord []byte) (*RollbackLogRecord, error) {
	r := RollbackLogRecord{}

	err := r.unmarshalBytes(rawRecord)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (lr *RollbackLogRecord) String() string {
	return fmt.Sprintf(`<ROLLBACK, %d>`, lr.TXNum())
}
