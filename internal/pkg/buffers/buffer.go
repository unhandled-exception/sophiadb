package buffers

import (
	"github.com/rotisserie/eris"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

// FailedToAssignBlockToBuffer — ошибка при связывании буыера с блоком
var FailedToAssignBlockToBuffer = eris.New("failed to assign a block to buffer")

// Buffer — страница в пуле буферов
type Buffer struct {
	fm       *storage.Manager
	lm       *wal.Manager
	contents *storage.Page
	block    *storage.BlockID
	pins     int
	txnum    int64
	lsn      int64
}

// NewBuffer создает новый объект буфера
func NewBuffer(fm *storage.Manager, lm *wal.Manager) *Buffer {
	buf := &Buffer{
		fm:       fm,
		lm:       lm,
		contents: storage.NewPage(fm.BlockSize()),
		pins:     0,
		txnum:    -1,
		lsn:      -1,
	}

	return buf
}

// Content возвращает страницу с соlержимым буфера
func (buf *Buffer) Content() *storage.Page {
	return buf.contents
}

// Block возвращает ссылку на блок
func (buf *Buffer) Block() *storage.BlockID {
	return buf.block
}

// SetModified устанавливает указатели транзакции и лога
func (buf *Buffer) SetModified(txnum int64, lsn int64) {
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
func (buf *Buffer) ModifyingTX() int64 {
	return buf.txnum
}

// AssignToBlock cвязывает страницу буфера со странице на диске
func (buf *Buffer) AssignToBlock(block *storage.BlockID) error {
	err := buf.Flush()
	if err != nil {
		return eris.Wrap(err, FailedToAssignBlockToBuffer.Error())
	}

	buf.block = block

	err = buf.fm.Read(buf.block, buf.contents)
	if err != nil {
		return eris.Wrap(err, FailedToAssignBlockToBuffer.Error())
	}

	return nil
}

// Flush сбрасывает страницу из памяти на диск
func (buf *Buffer) Flush() error {
	if buf.txnum >= 0 {
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
