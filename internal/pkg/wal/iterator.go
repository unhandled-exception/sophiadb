package wal

import (
	"github.com/rotisserie/eris"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
)

// Iterator — итератор по журналу
type Iterator struct {
	fm         *storage.FileManager
	blk        *storage.BlockID
	p          *storage.Page
	currentPos uint32
	boundary   uint32
}

// ErrFailedToCreateNewIterator — ошибка при создании нового итератора
var ErrFailedToCreateNewIterator = eris.New("failed to create a new wal iterator")

// NewIterator создает новый объект итератора по журналу
func NewIterator(fm *storage.FileManager, blk *storage.BlockID) (*Iterator, error) {
	it := &Iterator{
		fm:  fm,
		blk: blk,
		p:   storage.NewPage(fm.BlockSize()),
	}
	err := it.moveToBlock(blk)
	if err != nil {
		return nil, eris.Wrap(err, ErrFailedToCreateNewIterator.Error())
	}
	return it, nil
}

// HasNext возвращает признак возможности следующей итерации
func (it *Iterator) HasNext() bool {
	return it.currentPos < it.fm.BlockSize() || it.blk.Number() > 0
}

// Next достает следующею запись из лога
func (it *Iterator) Next() ([]byte, error) {
	if it.currentPos == it.fm.BlockSize() {
		it.blk = storage.NewBlockID(it.blk.Filename(), it.blk.Number()-1)
		it.moveToBlock(it.blk)
	}
	rec := it.p.GetBytes(it.currentPos)
	it.currentPos += int32Size + uint32(len(rec))
	return rec, nil
}

// Перемещаем итератор на следующий блок
func (it *Iterator) moveToBlock(blk *storage.BlockID) error {
	err := it.fm.Read(blk, it.p)
	if err != nil {
		return err
	}
	it.boundary = it.p.GetUint32(blockStart)
	it.currentPos = it.boundary
	return nil
}
