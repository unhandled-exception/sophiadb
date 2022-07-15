package types

import "fmt"

// Block описание блока хранилишща
type Block struct {
	Filename string
	Number   int32
}

// NewBlock создает новый объект BlockID
func NewBlock(filename string, number int32) Block {
	return Block{
		Filename: filename,
		Number:   number,
	}
}

// Equals сравнивает два блока на равенство
func (b Block) Equals(another Block) bool {
	return b.Filename == another.Filename && b.Number == another.Number
}

// String форматирует BlockID в строку
func (b Block) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename, b.Number)
}

// HashKey формирует строку с ключем для словарей
func (b Block) HashKey() string {
	return fmt.Sprintf("[%s][%d]", b.Filename, b.Number)
}
