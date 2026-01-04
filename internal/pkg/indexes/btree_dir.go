package indexes

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type BTreeDir struct {
	trx      scan.TRXInt
	layout   records.Layout
	contents *BTreePage
	filename string
}

func NewBTreeDir(trx scan.TRXInt, block types.Block, layout records.Layout) (*BTreeDir, error) {
	contents, err := NewBTreePage(trx, block, layout)
	if err != nil {
		return nil, err
	}

	dir := &BTreeDir{
		trx:      trx,
		layout:   layout,
		filename: block.Filename,
		contents: contents,
	}

	return dir, nil
}

func (d *BTreeDir) Close() {
	d.contents.Close()
}

func (d *BTreeDir) Search(searchKey scan.Constant) (types.BlockID, error) {
	childBlock, err := d.findChildBlock(searchKey)
	if err != nil {
		return -1, err
	}

	for {
		flag, err1 := d.contents.GetFlag()
		if err1 != nil {
			return -1, err1
		}

		if flag <= 0 {
			break
		}

		d.contents.Close()

		d.contents, err1 = NewBTreePage(d.trx, childBlock, d.layout)
		if err1 != nil {
			return -1, err1
		}

		childBlock, err1 = d.findChildBlock(searchKey)
		if err1 != nil {
			return -1, err1
		}
	}

	return childBlock.Number, nil
}

func (d *BTreeDir) findChildBlock(searchKey scan.Constant) (types.Block, error) {
	block := types.Block{
		Filename: d.filename,
	}

	slot, err := d.contents.FindSlotBefore(searchKey)
	if err != nil {
		return block, err
	}

	val, err := d.contents.GetVal(slot + 1)
	if err != nil {
		return block, err
	}

	if val.CompareTo(searchKey) == scan.CompEqual {
		slot++
	}

	block.Number, err = d.contents.GetChildNum(slot)
	if err != nil {
		return block, err
	}

	return block, nil
}

func (d *BTreeDir) MakeNewRoot(e *BTreeDirEntry) error {
	firstVal, err := d.contents.GetVal(0)
	if err != nil {
		return err
	}

	level, err := d.contents.GetFlag()
	if err != nil {
		return err
	}

	newBlock, err := d.contents.Split(0, level)
	if err != nil {
		return err
	}

	oldRoot := &BTreeDirEntry{
		BlockNumber: newBlock.Number,
		Dataval:     firstVal,
	}

	if _, err1 := d.insertEntry(oldRoot); err1 != nil {
		return err1
	}

	if _, err1 := d.insertEntry(e); err1 != nil {
		return err1
	}

	if err1 := d.contents.SetFlag(level + 1); err1 != nil {
		return err1
	}

	return nil
}

func (d *BTreeDir) Insert(e *BTreeDirEntry) (*BTreeDirEntry, error) {
	flag, err := d.contents.GetFlag()
	if err != nil {
		return nil, err
	}

	if flag == 0 {
		return d.insertEntry(e)
	}

	childBlock, err := d.findChildBlock(e.Dataval)
	if err != nil {
		return nil, err
	}

	child, err := NewBTreeDir(d.trx, childBlock, d.layout)
	if err != nil {
		return nil, err
	}

	myEntry, err := child.Insert(e)
	if err != nil {
		return nil, err
	}

	child.Close()

	if myEntry != nil {
		return d.insertEntry(myEntry)
	}

	return nil, nil //nolint:nilnil
}

func (d *BTreeDir) insertEntry(e *BTreeDirEntry) (*BTreeDirEntry, error) {
	newSlot, err := d.contents.FindSlotBefore(e.Dataval)
	if err != nil {
		return nil, err
	}

	newSlot++

	if err1 := d.contents.InsertDir(newSlot, e.Dataval, e.BlockNumber); err1 != nil {
		return nil, err
	}

	full, err := d.contents.IsFull()
	if err != nil || !full {
		return nil, err
	}

	// если страница заполнена, то расщепляем
	level, err := d.contents.GetFlag()
	if err != nil {
		return nil, err
	}

	records, err := d.contents.GetRecords()
	if err != nil {
		return nil, err
	}

	splitPos := types.SlotID(records / 2) //nolint:mnd
	splitVal, err := d.contents.GetVal(splitPos)
	if err != nil {
		return nil, err
	}

	newBlock, err := d.contents.Split(splitPos, level)
	if err != nil {
		return nil, err
	}

	return &BTreeDirEntry{
		BlockNumber: newBlock.Number,
		Dataval:     splitVal,
	}, nil
}
