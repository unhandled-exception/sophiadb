package transaction

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/concurrency"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

// const endOfFileBlock int32 = -1

type Transaction struct {
	txNum   types.TRX
	buffers buffersList
	rm      recoveryManager
	cm      concurrencyManager

	fm storageManager
	lm walManager
	bm buffersManager
}

func NewTransaction(nextTRX func() types.TRX, fm storageManager, lm walManager, bm buffersManager, lt concurrency.Lockers) (*Transaction, error) {
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
		return nil, err
	}

	t.rm = rm

	return t, nil
}

func (t *Transaction) Pin(block *types.Block) error {
	return nil
}

func (t *Transaction) Unpin(block *types.Block) error {
	return nil
}

func (t *Transaction) SetString(block *types.Block, offset uint32, value string, okToLog bool) error {
	return nil
}

func (t *Transaction) SetInt64(block *types.Block, offset uint32, value int64, okToLog bool) error {
	return nil
}

func (t *Transaction) TXNum() types.TRX {
	return t.txNum
}
