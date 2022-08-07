package records

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type (
	SlotFlag int8
)

var StartSlotID types.SlotID = -1

const (
	EmptySlot = 0
	UsedSlot  = 1
)

type RecordPage struct {
	Layout Layout
	TRX    trxInt
	Block  types.Block
}

func NewRecordPage(trx trxInt, block types.Block, layout Layout) (*RecordPage, error) {
	rp := &RecordPage{
		Layout: layout,
		TRX:    trx,
		Block:  block,
	}

	if err := rp.TRX.Pin(rp.Block); err != nil {
		return nil, errors.WithMessage(ErrRecordPage, err.Error())
	}

	return rp, nil
}

func (rp *RecordPage) GetInt64(slot types.SlotID, fieldName string) (int64, error) {
	if !rp.Layout.Schema.HasField(fieldName) {
		return 0, errors.WithMessagef(ErrFieldNotFound, "field %s", fieldName)
	}

	offset := rp.offset(slot) + rp.Layout.Offset(fieldName)

	val, err := rp.TRX.GetInt64(rp.Block, offset)
	if err != nil {
		return val, errors.WithMessage(ErrRecordPage, err.Error())
	}

	return val, nil
}

func (rp *RecordPage) GetString(slot types.SlotID, fieldName string) (string, error) {
	if !rp.Layout.Schema.HasField(fieldName) {
		return "", errors.WithMessagef(ErrFieldNotFound, "field %s", fieldName)
	}

	offset := rp.offset(slot) + rp.Layout.Offset(fieldName)

	val, err := rp.TRX.GetString(rp.Block, offset)
	if err != nil {
		return val, errors.WithMessage(ErrRecordPage, err.Error())
	}

	return val, nil
}

func (rp *RecordPage) GetInt8(slot types.SlotID, fieldName string) (int8, error) {
	if !rp.Layout.Schema.HasField(fieldName) {
		return 0, errors.WithMessagef(ErrFieldNotFound, "field %s", fieldName)
	}

	offset := rp.offset(slot) + rp.Layout.Offset(fieldName)

	val, err := rp.TRX.GetInt8(rp.Block, offset)
	if err != nil {
		return val, errors.WithMessage(ErrRecordPage, err.Error())
	}

	return val, nil
}

func (rp *RecordPage) SetInt64(slot types.SlotID, fieldName string, value int64) error {
	if !rp.Layout.Schema.HasField(fieldName) {
		return errors.WithMessagef(ErrFieldNotFound, "field %s", fieldName)
	}

	offset := rp.offset(slot) + rp.Layout.Offset(fieldName)

	if err := rp.TRX.SetInt64(rp.Block, offset, value, true); err != nil {
		return errors.WithMessage(ErrRecordPage, err.Error())
	}

	return nil
}

func (rp *RecordPage) SetString(slot types.SlotID, fieldName string, value string) error {
	if !rp.Layout.Schema.HasField(fieldName) {
		return errors.WithMessagef(ErrFieldNotFound, "field %s", fieldName)
	}

	offset := rp.offset(slot) + rp.Layout.Offset(fieldName)

	if err := rp.TRX.SetString(rp.Block, offset, value, true); err != nil {
		return errors.WithMessage(ErrRecordPage, err.Error())
	}

	return nil
}

func (rp *RecordPage) SetInt8(slot types.SlotID, fieldName string, value int8) error {
	if !rp.Layout.Schema.HasField(fieldName) {
		return errors.WithMessagef(ErrFieldNotFound, "field %s", fieldName)
	}

	offset := rp.offset(slot) + rp.Layout.Offset(fieldName)

	if err := rp.TRX.SetInt8(rp.Block, offset, value, true); err != nil {
		return errors.WithMessage(ErrRecordPage, err.Error())
	}

	return nil
}

func (rp *RecordPage) Format() (int32, error) {
	slot := types.SlotID(0)
	schema := rp.Layout.Schema

	for rp.isValidSlot(slot) {
		if err := rp.TRX.SetInt8(rp.Block, rp.offset(slot), EmptySlot, false); err != nil {
			return 0, errors.WithMessagef(ErrRecordPage, err.Error())
		}

		slotOffset := rp.offset(slot)
		for _, name := range schema.Fields() {
			pos := slotOffset + rp.Layout.Offset(name)

			var err error

			switch t := schema.Type(name); t {
			case Int64Field:
				err = rp.TRX.SetInt64(rp.Block, pos, 0, false)
			case Int8Field:
				err = rp.TRX.SetInt8(rp.Block, pos, 0, false)
			case StringField:
				err = rp.TRX.SetString(rp.Block, pos, "", false)
			default:
				err = fmt.Errorf("unknown filed type '%d' for field '%s'", t, name)
			}

			if err != nil {
				return 0, errors.WithMessagef(ErrRecordPage, err.Error())
			}
		}

		slot++
	}

	return int32(slot), nil
}

func (rp *RecordPage) Delete(slot types.SlotID) error {
	return rp.setFlag(slot, EmptySlot)
}

func (rp *RecordPage) NextAfter(slot types.SlotID) (types.SlotID, error) {
	return rp.searchAfter(slot, UsedSlot)
}

func (rp *RecordPage) InsertAfter(slot types.SlotID) (types.SlotID, error) {
	newSlot, err := rp.searchAfter(slot, EmptySlot)
	if err != nil {
		return StartSlotID, err
	}

	if err := rp.setFlag(newSlot, UsedSlot); err != nil {
		return StartSlotID, err
	}

	return newSlot, nil
}

func (rp *RecordPage) offset(slot types.SlotID) uint32 {
	return uint32(slot) * rp.Layout.SlotSize
}

func (rp *RecordPage) setFlag(slot types.SlotID, flag SlotFlag) error {
	return rp.TRX.SetInt8(rp.Block, rp.offset(slot), int8(flag), true)
}

func (rp *RecordPage) isValidSlot(slot types.SlotID) bool {
	return rp.offset(slot+1) <= rp.TRX.BlockSize()
}

func (rp *RecordPage) searchAfter(slot types.SlotID, flag SlotFlag) (types.SlotID, error) {
	slot++
	for rp.isValidSlot(slot) {
		f, err := rp.TRX.GetInt8(rp.Block, rp.offset(slot))
		if err != nil {
			return -1, errors.WithMessagef(ErrRecordPage, err.Error())
		}

		if SlotFlag(f) == flag {
			return slot, nil
		}

		slot++
	}

	return -1, ErrSlotNotFound
}
