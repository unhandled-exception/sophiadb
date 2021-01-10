package storage

import "fmt"

// BlockID описание блока хранилишща
type BlockID struct {
	filename string
	number   uint32
}

// NewBlockID создает новый объект BlockID
func NewBlockID(filename string, number uint32) *BlockID {
	return &BlockID{
		filename: filename,
		number:   number,
	}
}

// Filename возвращает поле filename
func (bid *BlockID) Filename() string {
	return bid.filename
}

// Number возвращает поле number
func (bid *BlockID) Number() uint32 {
	return bid.number
}

// Equals сравнивает два блока на равенство
func (bid *BlockID) Equals(another *BlockID) bool {
	return bid.filename == another.Filename() && bid.number == another.Number()
}

// String форматирует BlockID в строку
func (bid *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", bid.filename, bid.number)
}

// HashKey формирует строку с ключем для словарей
func (bid *BlockID) HashKey() string {
	return fmt.Sprintf("[%s][%d]", bid.filename, bid.number)
}
