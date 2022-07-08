package buffers

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

// Buffer — страница в пуле буферов
type Buffer struct {
	fm       *storage.Manager
	lm       *wal.Manager
	contents *types.Page
	block    *types.Block
	pins     int
	txnum    types.TRX
	lsn      types.LSN
	mu       sync.Mutex
}

// NewBuffer создает новый объект буфера
func NewBuffer(fm *storage.Manager, lm *wal.Manager) *Buffer {
	buf := &Buffer{
		fm:       fm,
		lm:       lm,
		contents: types.NewPage(fm.BlockSize()),
		pins:     0,
		txnum:    -1,
		lsn:      -1,
	}

	return buf
}

// Content возвращает страницу с соlержимым буфера
func (buf *Buffer) Content() *types.Page {
	return buf.contents
}

// Block возвращает ссылку на блок
func (buf *Buffer) Block() *types.Block {
	return buf.block
}

// SetModified устанавливает указатели транзакции и лога
func (buf *Buffer) SetModified(txnum types.TRX, lsn types.LSN) {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	buf.txnum = txnum
	if lsn >= 0 {
		buf.lsn = lsn
	}
}

// Pin закрепляет страницу в памяти и увеличивает счетчик закрпелений
func (buf *Buffer) Pin() {
	buf.pins++
}

// Unpin уменьщает счетчик закреплений в памяти
func (buf *Buffer) Unpin() {
	buf.pins--
}

// IsPinned возвращает признак закрплена страница или нет
func (buf *Buffer) IsPinned() bool {
	return buf.pins > 0
}

// ModifyingTX возвращает указатель транзакции
func (buf *Buffer) ModifyingTX() types.TRX {
	buf.mu.Lock()
	defer buf.mu.Unlock()

	return buf.txnum
}

// Возвращает LSN
func (buf *Buffer) LSN() types.LSN {
	return buf.lsn
}

// Возвращает LSN
func (buf *Buffer) Pins() int {
	return buf.pins
}

// AssignToBlock cвязывает страницу буфера со странице на диске
func (buf *Buffer) AssignToBlock(block *types.Block) error {
	err := buf.Flush()
	if err != nil {
		return errors.WithMessage(ErrFailedToAssignBlockToBuffer, err.Error())
	}

	buf.block = block

	err = buf.fm.Read(buf.block, buf.contents)
	if err != nil {
		return errors.WithMessage(ErrFailedToAssignBlockToBuffer, err.Error())
	}

	return nil
}

// Flush сбрасывает страницу из памяти на диск
func (buf *Buffer) Flush() error {
	if buf.txnum >= 0 {
		buf.mu.Lock()
		defer buf.mu.Unlock()

		err := buf.lm.Flush(buf.lsn, false)
		if err != nil {
			return err
		}

		err = buf.fm.Write(buf.block, buf.contents)
		if err != nil {
			return err
		}

		buf.txnum = -1
	}

	return nil
}
