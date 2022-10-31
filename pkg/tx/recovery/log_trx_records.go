package recovery

import (
	"fmt"

	"github.com/unhandled-exception/sophiadb/pkg/types"
)

type StartLogRecord struct {
	BaseLogRecord
}

func NewStartLogRecord(txnum types.TRX) StartLogRecord {
	return StartLogRecord{
		BaseLogRecord: BaseLogRecord{
			op:    StartOp,
			txnum: txnum,
		},
	}
}

func NewStartLogRecordFromBytes(rawRecord []byte) (StartLogRecord, error) {
	r := StartLogRecord{}

	if err := r.unmarshalBytes(rawRecord); err != nil {
		return r, err
	}

	return r, nil
}

func (lr StartLogRecord) String() string {
	return fmt.Sprintf(`<START, %d>`, lr.TXNum())
}

type CommitLogRecord struct {
	BaseLogRecord
}

func NewCommitLogRecord(txnum types.TRX) CommitLogRecord {
	return CommitLogRecord{
		BaseLogRecord: BaseLogRecord{
			op:    CommitOp,
			txnum: txnum,
		},
	}
}

func NewCommitLogRecordFromBytes(rawRecord []byte) (CommitLogRecord, error) {
	r := CommitLogRecord{}

	if err := r.unmarshalBytes(rawRecord); err != nil {
		return r, err
	}

	return r, nil
}

func (lr CommitLogRecord) String() string {
	return fmt.Sprintf(`<COMMIT, %d>`, lr.TXNum())
}

type RollbackLogRecord struct {
	BaseLogRecord
}

func NewRollbackLogRecord(txnum types.TRX) RollbackLogRecord {
	return RollbackLogRecord{
		BaseLogRecord: BaseLogRecord{
			op:    RollbackOp,
			txnum: txnum,
		},
	}
}

func NewRollbackLogRecordFromBytes(rawRecord []byte) (RollbackLogRecord, error) {
	r := RollbackLogRecord{}

	if err := r.unmarshalBytes(rawRecord); err != nil {
		return r, err
	}

	return r, nil
}

func (lr RollbackLogRecord) String() string {
	return fmt.Sprintf(`<ROLLBACK, %d>`, lr.TXNum())
}
