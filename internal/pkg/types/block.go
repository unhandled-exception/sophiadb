package types

import "fmt"

// Block описание блока хранилишща
type Block struct {
	Filename string
	Number   int32
}

// Equals сравнивает два блока на равенство
func (b Block) Equals(another Block) bool {
	return b == another
}

// String форматирует BlockID в строку
func (b Block) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename, b.Number)
}
