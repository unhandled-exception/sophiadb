package transaction

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/concurrency"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

const endOfFileBlock int32 = -1

type Transaction struct {
	txNum   types.TRX
	buffers bufferList
	rm      recoveryManager
	cm      concurrencyManager

	fm storageManager
	lm logManager
	bm buffersManager
}

func NewTransaction(nextTRX func() types.TRX, fm storageManager, lm logManager, bm buffersManager, lt concurrency.Lockers) (*Transaction, error) {
	txNum := nextTRX()

	t := &Transaction{
		txNum:   txNum,
		buffers: NewBuffersList(bm),
		cm:      concurrency.NewManager(lt),
		fm:      fm,
		lm:      lm,
		bm:      bm,
	}

	rm, err := recovery.NewManager(t, lm, bm)
	if err != nil {
		return nil, t.wrapTransactionError(err)
	}

	t.rm = rm

	return t, nil
}

func (t *Transaction) TXNum() types.TRX {
	return t.txNum
}

func (t *Transaction) Commit() error {
	if err := t.rm.Commit(); err != nil {
		return t.wrapTransactionError(err)
	}

	t.cm.Release()
	t.buffers.UnpinAll()

	return nil
}

func (t *Transaction) Rollback() error {
	if err := t.rm.Rollback(); err != nil {
		return t.wrapTransactionError(err)
	}

	t.cm.Release()
	t.buffers.UnpinAll()

	return nil
}

func (t *Transaction) Recover() error {
	if err := t.bm.FlushAll(t.txNum); err != nil {
		return t.wrapTransactionError(err)
	}

	if err := t.rm.Recover(); err != nil {
		return t.wrapTransactionError(err)
	}

	return nil
}

func (t *Transaction) Pin(block types.Block) error {
	return t.wrapTransactionError(t.buffers.Pin(block))
}

func (t *Transaction) Unpin(block types.Block) {
	t.buffers.Unpin(block)
}

func (t *Transaction) GetInt64(block types.Block, offset uint32) (int64, error) {
	if err := t.cm.SLock(block); err != nil {
		return 0, t.wrapTransactionError(err)
	}

	buf := t.buffers.GetBuffer(block)

	return buf.Content().GetInt64(offset), nil
}

func (t *Transaction) GetString(block types.Block, offset uint32) (string, error) {
	if err := t.cm.SLock(block); err != nil {
		return "", t.wrapTransactionError(err)
	}

	buf := t.buffers.GetBuffer(block)

	return buf.Content().GetString(offset), nil
}

func (t *Transaction) SetInt64(block types.Block, offset uint32, value int64, okToLog bool) error {
	if err := t.cm.XLock(block); err != nil {
		return t.wrapTransactionError(err)
	}

	buf := t.buffers.GetBuffer(block)
	lsn := types.LSN(-1)

	if okToLog {
		var err error

		lsn, err = t.rm.SetInt64(buf, offset, value)
		if err != nil {
			return t.wrapTransactionError(err)
		}
	}

	buf.Content().SetInt64(offset, value)
	buf.SetModified(t.txNum, lsn)

	return nil
}

func (t *Transaction) SetString(block types.Block, offset uint32, value string, okToLog bool) error {
	if err := t.cm.XLock(block); err != nil {
		return t.wrapTransactionError(err)
	}

	buf := t.buffers.GetBuffer(block)
	lsn := types.LSN(-1)

	if okToLog {
		var err error

		lsn, err = t.rm.SetString(buf, offset, value)
		if err != nil {
			return t.wrapTransactionError(err)
		}
	}

	buf.Content().SetString(offset, value)
	buf.SetModified(t.txNum, lsn)

	return nil
}

func (t *Transaction) BlockSize() uint32 {
	return t.fm.BlockSize()
}

func (t *Transaction) AvailableBuffersCount() int {
	return t.bm.Available()
}

func (t *Transaction) Size(filename string) (int32, error) {
	dummyBlock := types.Block{Filename: filename, Number: endOfFileBlock}

	if err := t.cm.SLock(dummyBlock); err != nil {
		return 0, t.wrapTransactionError(err)
	}

	return t.fm.Length(filename)
}

func (t *Transaction) Append(filename string) (types.Block, error) {
	dummyBlock := types.Block{Filename: filename, Number: endOfFileBlock}

	if err := t.cm.XLock(dummyBlock); err != nil {
		return types.Block{}, t.wrapTransactionError(err)
	}

	return t.fm.Append(filename)
}

func (t *Transaction) wrapTransactionError(err error) error {
	if err == nil {
		return nil
	}

	return errors.WithMessagef(ErrTransactionFailed, "trx_id %d: %s", t.txNum, err)
}
