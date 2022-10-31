package recovery

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

type Manager struct {
	trx trxInt
	bm  BuffersManager
	lm  LogManager
}

func NewManager(trx trxInt, lm LogManager, bm BuffersManager) (*Manager, error) {
	m := Manager{
		trx: trx,
		bm:  bm,
		lm:  lm,
	}

	if err := m.start(); err != nil {
		return nil, err
	}

	return &m, nil
}

func (m *Manager) start() error {
	txnum := m.trx.TXNum()

	if err := m.bm.FlushAll(txnum); err != nil {
		return errors.WithMessage(ErrOpError, err.Error())
	}

	lr := NewStartLogRecord(txnum)

	lsn, err := m.writeRecordToLog(lr)
	if err != nil {
		return err
	}

	if err := m.lm.Flush(lsn, false); err != nil {
		return errors.WithMessage(ErrOpError, err.Error())
	}

	return nil
}

func (m *Manager) Commit() error {
	txnum := m.trx.TXNum()

	if err := m.bm.FlushAll(txnum); err != nil {
		return errors.WithMessage(ErrOpError, err.Error())
	}

	lr := NewCommitLogRecord(txnum)

	lsn, err := m.writeRecordToLog(lr)
	if err != nil {
		return err
	}

	if err := m.lm.Flush(lsn, false); err != nil {
		return errors.WithMessage(ErrOpError, err.Error())
	}

	return nil
}

func (m *Manager) Rollback() error {
	txnum := m.trx.TXNum()

	if err := m.doRollback(); err != nil {
		return errors.WithMessage(ErrOpError, err.Error())
	}

	if err := m.bm.FlushAll(txnum); err != nil {
		return errors.WithMessage(ErrOpError, err.Error())
	}

	lr := NewRollbackLogRecord(txnum)

	lsn, err := m.writeRecordToLog(lr)
	if err != nil {
		return err
	}

	if err := m.lm.Flush(lsn, false); err != nil {
		return errors.WithMessage(ErrOpError, err.Error())
	}

	return nil
}

func (m *Manager) Recover() error {
	txnum := m.trx.TXNum()

	if err := m.doRecover(); err != nil {
		return errors.WithMessage(ErrOpError, err.Error())
	}

	if err := m.bm.FlushAll(txnum); err != nil {
		return errors.WithMessage(ErrOpError, err.Error())
	}

	lr := NewCheckpointLogRecord()

	lsn, err := m.writeRecordToLog(lr)
	if err != nil {
		return err
	}

	if err := m.lm.Flush(lsn, false); err != nil {
		return errors.WithMessage(ErrOpError, err.Error())
	}

	return nil
}

func (m *Manager) SetInt64(buf buffer, offset uint32, value int64) (types.LSN, error) {
	txnum := m.trx.TXNum()

	oldValue := buf.Content().GetInt64(offset)
	block := buf.Block()

	lr := NewSetInt64LogRecord(txnum, block, offset, oldValue)

	return m.writeRecordToLog(lr)
}

func (m *Manager) SetInt8(buf buffer, offset uint32, value int8) (types.LSN, error) {
	txnum := m.trx.TXNum()

	oldValue := buf.Content().GetInt8(offset)
	block := buf.Block()

	lr := NewSetInt8LogRecord(txnum, block, offset, oldValue)

	return m.writeRecordToLog(lr)
}

func (m *Manager) SetString(buf buffer, offset uint32, value string) (types.LSN, error) {
	txnum := m.trx.TXNum()

	oldValue := buf.Content().GetString(offset)
	block := buf.Block()

	lr := NewSetStringLogRecord(txnum, block, offset, oldValue)

	return m.writeRecordToLog(lr)
}

func (m *Manager) doRollback() error {
	txnum := m.trx.TXNum()

	it, err := m.lm.Iterator()
	if err != nil {
		return err
	}

	for it.HasNext() {
		raw, err := it.Next()
		if err != nil {
			return err
		}

		lr, err := NewLogRecordFromBytes(raw)
		if err != nil {
			return err
		}

		switch {
		case lr.TXNum() != txnum:
			continue
		case lr.Op() == StartOp:
			break
		default:
			if err := lr.Undo(m.trx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Manager) doRecover() error {
	it, err := m.lm.Iterator()
	if err != nil {
		return err
	}

	finishedTrxs := make(map[types.TRX]struct{})
	foundCheckpoint := false

	for !foundCheckpoint && it.HasNext() {
		raw, err := it.Next()
		if err != nil {
			return err
		}

		lr, err := NewLogRecordFromBytes(raw)
		if err != nil {
			return err
		}

		switch {
		case lr.Op() == CheckpointOp:
			foundCheckpoint = true
		case lr.Op() == CommitOp || lr.Op() == RollbackOp:
			finishedTrxs[lr.TXNum()] = struct{}{}
		default:
			_, ok := finishedTrxs[lr.TXNum()]
			if !ok {
				if err := lr.Undo(m.trx); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (m *Manager) writeRecordToLog(lr LogRecord) (types.LSN, error) {
	lsn, err := m.lm.Append(lr.MarshalBytes())
	if err != nil {
		return -1, errors.WithMessage(ErrOpError, err.Error())
	}

	return lsn, nil
}
