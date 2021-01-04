package storage

import "fmt"

// BlockID описание блока хранилишща
type BlockID struct {
	filename string
	blkNum   uint64
}

// NewBlockID создает новый объект BlockID
func NewBlockID(filename string, blkNum uint64) *BlockID {
	return &BlockID{
		filename: filename,
		blkNum:   blkNum,
	}
}

// Filename возвращает поле filename
func (bid *BlockID) Filename() string {
	return bid.filename
}

// BlkNum возвращает поле blkNum
func (bid *BlockID) BlkNum() uint64 {
	return bid.blkNum
}

// Equals сравнивает два блока на равенство
func (bid *BlockID) Equals(another *BlockID) bool {
	return bid.filename == another.Filename() && bid.blkNum == another.BlkNum()
}

// String форматирует BlockID в строку
func (bid *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", bid.filename, bid.blkNum)
}

// HashKey формирует строку с ключем для словарей
func (bid *BlockID) HashKey() string {
	return fmt.Sprintf("[%s][%d]", bid.filename, bid.blkNum)
}
