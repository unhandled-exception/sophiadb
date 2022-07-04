package types

import "fmt"

// Block описание блока хранилишща
type Block struct {
	filename string
	number   uint32
}

// NewBlockID создает новый объект BlockID
func NewBlockID(filename string, number uint32) *Block {
	return &Block{
		filename: filename,
		number:   number,
	}
}

// Filename возвращает поле filename
func (bid *Block) Filename() string {
	return bid.filename
}

// Number возвращает поле number
func (bid *Block) Number() uint32 {
	return bid.number
}

// Equals сравнивает два блока на равенство
func (bid *Block) Equals(another *Block) bool {
	return bid.filename == another.Filename() && bid.number == another.Number()
}

// String форматирует BlockID в строку
func (bid *Block) String() string {
	return fmt.Sprintf("[file %s, block %d]", bid.filename, bid.number)
}

// HashKey формирует строку с ключем для словарей
func (bid *Block) HashKey() string {
	return fmt.Sprintf("[%s][%d]", bid.filename, bid.number)
}
