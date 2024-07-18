package indexes

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type BTreePage struct {
	trx      scan.TRXInt
	curBlock *types.Block
	layout   records.Layout

	recordsCountOffset uint32
	flagOffset         uint32
	dataOffset         uint32
}

const (
	idFieldName      = "id"
	blockFieldName   = "block"
	datavalFieldName = "dataval"
)

func NewBTreePage(trx scan.TRXInt, block types.Block, layout records.Layout) (*BTreePage, error) {
	page := &BTreePage{
		trx:      trx,
		curBlock: &block,
		layout:   layout,

		recordsCountOffset: types.Int64Size,
		flagOffset:         0,
		dataOffset:         2 * types.Int64Size, //nolint:mnd
	}

	if err := trx.Pin(block); err != nil {
		return nil, err
	}

	return page, nil
}

func (p *BTreePage) Close() {
	if p.curBlock != nil {
		p.trx.Unpin(*p.curBlock)
	}

	p.curBlock = nil
}

func (p *BTreePage) FindSlotBefore(searchKey scan.Constant) (types.SlotID, error) {
	var slot types.SlotID = 0

	records, err := p.GetRecords()
	if err != nil {
		return 0, err
	}

	for slot < types.SlotID(records) {
		value, err1 := p.GetVal(slot)
		if err1 != nil {
			return -1, err1
		}

		if value.CompareTo(searchKey) == scan.CompLess {
			slot++
		} else {
			break
		}
	}

	return slot - 1, nil
}

func (p *BTreePage) IsFull() (bool, error) {
	records, err := p.GetRecords()
	if err != nil {
		return false, err
	}

	pos := p.slotPos(types.SlotID(records) + 1)

	return pos >= int32(p.trx.BlockSize()), nil
}

func (p *BTreePage) Split(splitPos types.SlotID, flag int64) (types.Block, error) {
	block, err := p.AppendNewBlock(flag)
	if err != nil {
		return types.Block{}, err
	}

	newPage, err := NewBTreePage(p.trx, block, p.layout)
	if err != nil {
		return types.Block{}, err
	}

	if err = p.transferRecords(splitPos, newPage); err != nil {
		return types.Block{}, err
	}

	if err := newPage.SetFlag(flag); err != nil {
		return types.Block{}, err
	}

	newPage.Close()

	return block, nil
}

func (p *BTreePage) GetFlag() (int64, error) {
	return p.trx.GetInt64(*p.curBlock, p.flagOffset)
}

func (p *BTreePage) SetFlag(value int64) error {
	return p.trx.SetInt64(*p.curBlock, p.flagOffset, value, true)
}

func (p *BTreePage) AppendNewBlock(flag int64) (types.Block, error) {
	block, err := p.trx.Append(p.curBlock.Filename)
	if err != nil {
		return types.Block{}, err
	}

	if err = p.trx.Pin(block); err != nil {
		return types.Block{}, err
	}

	if err = p.FormatBlock(block, flag); err != nil {
		return types.Block{}, err
	}

	return block, nil
}

func (p *BTreePage) FormatBlock(block types.Block, flag int64) error {
	if err := p.trx.SetInt64(block, p.flagOffset, flag, false); err != nil {
		return err
	}

	if err := p.trx.SetInt64(block, p.recordsCountOffset, 0, false); err != nil {
		return err
	}

	recSize := p.layout.SlotSize
	blockSize := p.trx.BlockSize()

	for pos := p.dataOffset; pos+recSize < blockSize; pos += recSize {
		if err1 := p.makeDefaultBlockRecord(block, pos); err1 != nil {
			return err1
		}
	}

	return nil
}

func (p *BTreePage) makeDefaultBlockRecord(block types.Block, pos uint32) error {
	for _, fieldName := range p.layout.Schema.Fields() {
		var err error

		//nolint:exhaustive
		switch p.layout.Schema.Type(fieldName) {
		case records.Int64Field:
			err = p.trx.SetInt64(block, pos+p.layout.Offset(fieldName), 0, false)
		case records.Int8Field:
			err = p.trx.SetInt8(block, pos+p.layout.Offset(fieldName), 0, false)
		case records.StringField:
			err = p.trx.SetString(block, pos+p.layout.Offset(fieldName), "", false)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (p *BTreePage) GetRecords() (int64, error) {
	return p.trx.GetInt64(*p.curBlock, p.recordsCountOffset)
}

func (p *BTreePage) InsertDir(slot types.SlotID, value scan.Constant, blockID types.BlockID) error {
	if err := p.Insert(slot); err != nil {
		return err
	}

	if err := p.SetVal(slot, datavalFieldName, value); err != nil {
		return err
	}

	if err := p.setInt64(slot, blockFieldName, int64(blockID)); err != nil {
		return err
	}

	return nil
}

func (p *BTreePage) GetChildNum(slot types.SlotID) (types.BlockID, error) {
	block, err := p.getInt64(slot, blockFieldName)

	return types.BlockID(block), err
}

func (p *BTreePage) GetDataRID(slot types.SlotID) (types.RID, error) {
	blockID, err := p.getInt64(slot, blockFieldName)
	if err != nil {
		return types.RID{}, err
	}

	id, err := p.getInt64(slot, idFieldName)
	if err != nil {
		return types.RID{}, err
	}

	return types.RID{
		BlockNumber: types.BlockID(blockID),
		Slot:        types.SlotID(id),
	}, nil
}

func (p *BTreePage) InsertLeaf(slot types.SlotID, value scan.Constant, rid types.RID) error {
	if err := p.Insert(slot); err != nil {
		return err
	}

	if err := p.SetVal(slot, datavalFieldName, value); err != nil {
		return err
	}

	if err := p.setInt64(slot, blockFieldName, int64(rid.BlockNumber)); err != nil {
		return err
	}

	if err := p.setInt64(slot, idFieldName, int64(rid.Slot)); err != nil {
		return err
	}

	return nil
}

func (p *BTreePage) Delete(slot types.SlotID) error {
	records, err := p.GetRecords()
	if err != nil {
		return err
	}

	for i := int32(slot) + 1; i < int32(records); i++ {
		if err = p.copyRecord(types.SlotID(i), types.SlotID(i-1)); err != nil {
			return err
		}
	}

	if err = p.setRecords(records - 1); err != nil {
		return err
	}

	return nil
}

func (p *BTreePage) Insert(slot types.SlotID) error {
	records, err := p.GetRecords()
	if err != nil {
		return err
	}

	for i := int32(records); i > int32(slot); i-- {
		if err = p.copyRecord(types.SlotID(i-1), types.SlotID(i)); err != nil {
			return err
		}
	}

	return p.setRecords(records + 1)
}

func (p *BTreePage) GetVal(slot types.SlotID) (scan.Constant, error) {
	return p.getVal(slot, datavalFieldName)
}

func (p *BTreePage) SetVal(slot types.SlotID, field string, value scan.Constant) error {
	//nolint:exhaustive
	switch p.layout.Schema.Type(field) {
	case records.Int64Field:
		if val, ok := value.Value().(int64); ok {
			return p.setInt64(slot, field, val)
		}
	case records.Int8Field:
		if val, ok := value.Value().(int8); ok {
			return p.setInt8(slot, field, val)
		}
	case records.StringField:
		if val, ok := value.Value().(string); ok {
			return p.setString(slot, field, val)
		}
	}

	return ErrUnknownFieldType
}

func (p *BTreePage) setRecords(records int64) error {
	return p.trx.SetInt64(*p.curBlock, p.recordsCountOffset, records, true)
}

func (p *BTreePage) getInt64(slot types.SlotID, field string) (int64, error) {
	return p.trx.GetInt64(*p.curBlock, uint32(p.fieldPos(slot, field)))
}

func (p *BTreePage) getInt8(slot types.SlotID, field string) (int8, error) {
	return p.trx.GetInt8(*p.curBlock, uint32(p.fieldPos(slot, field)))
}

func (p *BTreePage) getString(slot types.SlotID, field string) (string, error) {
	return p.trx.GetString(*p.curBlock, uint32(p.fieldPos(slot, field)))
}

func (p *BTreePage) getVal(slot types.SlotID, field string) (scan.Constant, error) {
	//nolint:exhaustive
	switch p.layout.Schema.Type(field) {
	case records.Int64Field:
		value, err := p.getInt64(slot, field)
		if err != nil {
			return nil, err
		}

		return scan.NewInt64Constant(value), nil
	case records.Int8Field:
		value, err := p.getInt8(slot, field)
		if err != nil {
			return nil, err
		}

		return scan.NewInt8Constant(value), nil
	case records.StringField:
		value, err := p.getString(slot, field)
		if err != nil {
			return nil, err
		}

		return scan.NewStringConstant(value), nil
	default:
		return nil, ErrUnknownFieldType
	}
}

func (p *BTreePage) setInt64(slot types.SlotID, field string, value int64) error {
	return p.trx.SetInt64(*p.curBlock, uint32(p.fieldPos(slot, field)), value, true)
}

func (p *BTreePage) setInt8(slot types.SlotID, field string, value int8) error {
	return p.trx.SetInt8(*p.curBlock, uint32(p.fieldPos(slot, field)), value, true)
}

func (p *BTreePage) setString(slot types.SlotID, field string, value string) error {
	return p.trx.SetString(*p.curBlock, uint32(p.fieldPos(slot, field)), value, true)
}

func (p *BTreePage) copyRecord(from, to types.SlotID) error {
	for _, fieldName := range p.layout.Schema.Fields() {
		value, err := p.getVal(from, fieldName)
		if err != nil {
			return err
		}

		if err = p.SetVal(to, fieldName, value); err != nil {
			return err
		}
	}

	return nil
}

func (p *BTreePage) fieldPos(slot types.SlotID, field string) int32 {
	return p.slotPos(slot) + int32(p.layout.Offset(field))
}

func (p *BTreePage) slotPos(slot types.SlotID) int32 {
	return int32(p.dataOffset) + (int32(slot) * int32(p.layout.SlotSize))
}

func (p *BTreePage) transferRecords(splitPos types.SlotID, dest *BTreePage) error {
	var destSlot types.SlotID = 0

	schema := p.layout.Schema

	for {
		records, err := p.GetRecords()
		if err != nil {
			return err
		}

		if types.SlotID(records) <= splitPos {
			break
		}

		if err = dest.Insert(destSlot); err != nil {
			return err
		}

		for _, fieldName := range schema.Fields() {
			value, err1 := p.getVal(splitPos, fieldName)
			if err1 != nil {
				return err1
			}

			if err1 = dest.SetVal(destSlot, fieldName, value); err1 != nil {
				return err
			}
		}

		if err = p.Delete(splitPos); err != nil {
			return err
		}

		destSlot++
	}

	return nil
}
