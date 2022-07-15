package wal

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

// Iterator — итератор по журналу
type Iterator struct {
	fm         *storage.Manager
	blk        types.Block
	p          *types.Page
	currentPos uint32
	boundary   uint32
}

// NewIterator создает новый объект итератора по журналу
func NewIterator(fm *storage.Manager, blk types.Block) (*Iterator, error) {
	it := &Iterator{
		fm:  fm,
		blk: blk,
		p:   types.NewPage(fm.BlockSize()),
	}

	if err := it.moveToBlock(blk); err != nil {
		return nil, errors.WithMessage(ErrFailedToCreateNewIterator, err.Error())
	}

	return it, nil
}

// HasNext возвращает признак возможности следующей итерации
func (it *Iterator) HasNext() bool {
	return it.currentPos < it.fm.BlockSize() || it.blk.Number > 0
}

// Next достает следующею запись из лога
func (it *Iterator) Next() ([]byte, error) {
	if it.currentPos == it.fm.BlockSize() {
		it.blk = types.NewBlock(it.blk.Filename, it.blk.Number-1)

		err := it.moveToBlock(it.blk)
		if err != nil {
			return nil, err
		}
	}

	rec := it.p.GetBytes(it.currentPos)
	it.currentPos += int32Size + uint32(len(rec))

	return rec, nil
}

// Перемещаем итератор на следующий блок
func (it *Iterator) moveToBlock(blk types.Block) error {
	if err := it.fm.Read(blk, it.p); err != nil {
		return err
	}

	it.boundary = it.p.GetUint32(blockStart)
	it.currentPos = it.boundary

	return nil
}
