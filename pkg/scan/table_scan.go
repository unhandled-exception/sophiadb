package scan

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

const tableSuffix = ".tbl"

type TableScan struct {
	trx      TRXInt
	Filename string
	Layout   records.Layout

	rp          *records.RecordPage
	currentSlot types.SlotID
}

func NewTableScan(trx TRXInt, filename string, layout records.Layout) (*TableScan, error) {
	filename = filename + tableSuffix

	ts := &TableScan{
		trx:      trx,
		Filename: filename,
		Layout:   layout,
	}

	size, err := trx.Size(filename)
	if err != nil {
		return nil, errors.WithMessage(ErrTableScan, err.Error())
	}

	if size == 0 {
		if err := ts.moveToNewBlock(); err != nil {
			return nil, err
		}
	} else {
		if err := ts.moveToBlock(0); err != nil {
			return nil, err
		}
	}

	return ts, nil
}

func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.trx.Unpin(ts.rp.Block)
	}
}

func (ts *TableScan) ForEach(call func() (bool, error)) error {
	if err := ts.BeforeFirst(); err != nil {
		return err
	}

	for {
		ok, err := ts.Next()
		if !ok {
			if err != nil {
				return err
			}

			break
		}

		stop, err := call()
		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

func (ts *TableScan) ForEachField(call func(name string, fieldType records.FieldType) (bool, error)) error {
	for _, name := range ts.Layout.Schema.Fields() {
		fieldType := ts.Layout.Schema.Type(name)

		stop, err := call(name, fieldType)
		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

func (ts *TableScan) ForEachValue(call func(name string, fieldType records.FieldType, value interface{}) (bool, error)) error {
	for _, name := range ts.Layout.Schema.Fields() {
		fieldType := ts.Layout.Schema.Type(name)

		var (
			value interface{}
			err   error
		)

		switch fieldType {
		case records.Int64Field:
			value, err = ts.GetInt64(name)
		case records.Int8Field:
			value, err = ts.GetInt8(name)
		case records.StringField:
			value, err = ts.GetString(name)
		default:
			err = ErrUnknownFieldType
		}

		if err != nil {
			return err
		}

		stop, err := call(name, fieldType, value)
		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

func (ts *TableScan) BeforeFirst() error {
	return ts.moveToBlock(0)
}

func (ts *TableScan) Next() (bool, error) {
	currentSlot, err := ts.rp.NextAfter(ts.currentSlot)
	if err != nil && !errors.Is(err, records.ErrSlotNotFound) {
		return false, errors.WithMessage(ErrTableScan, err.Error())
	}

	ts.currentSlot = currentSlot

	for ts.currentSlot < 0 {
		ok, err := ts.atLastBlock()
		if err != nil {
			return false, err
		}

		if ok {
			return false, nil
		}

		if err = ts.moveToBlock(ts.rp.Block.Number + 1); err != nil {
			return false, err
		}

		currentSlot, err = ts.rp.NextAfter(ts.currentSlot)
		if err != nil {
			return false, errors.WithMessage(ErrTableScan, err.Error())
		}

		ts.currentSlot = currentSlot
	}

	return true, nil
}

func (ts *TableScan) GetInt64(fieldName string) (int64, error) {
	val, err := ts.rp.GetInt64(ts.currentSlot, fieldName)
	if err != nil {
		return 0, errors.WithMessage(ErrTableScan, err.Error())
	}

	return val, nil
}

func (ts *TableScan) GetInt8(fieldName string) (int8, error) {
	val, err := ts.rp.GetInt8(ts.currentSlot, fieldName)
	if err != nil {
		return 0, errors.WithMessage(ErrTableScan, err.Error())
	}

	return val, nil
}

func (ts *TableScan) GetString(fieldName string) (string, error) {
	val, err := ts.rp.GetString(ts.currentSlot, fieldName)
	if err != nil {
		return "", errors.WithMessage(ErrTableScan, err.Error())
	}

	return val, nil
}

func (ts *TableScan) GetVal(fieldName string) (Constant, error) {
	switch t := ts.Layout.Schema.Type(fieldName); t {
	case records.Int64Field:
		val, err := ts.GetInt64(fieldName)
		if err != nil {
			return nil, err
		}

		return NewInt64Constant(val), nil
	case records.Int8Field:
		val, err := ts.GetInt8(fieldName)
		if err != nil {
			return nil, err
		}

		return NewInt8Constant(val), nil
	case records.StringField:
		val, err := ts.GetString(fieldName)
		if err != nil {
			return nil, err
		}

		return NewStringConstant(val), nil
	default:
		return nil, errors.WithMessagef(ErrTableScan, "unknown field type %d for field '%s'", t, fieldName)
	}
}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.Layout.Schema.HasField(fieldName)
}

func (ts *TableScan) SetInt64(fieldName string, value int64) error {
	if err := ts.rp.SetInt64(ts.currentSlot, fieldName, value); err != nil {
		return errors.WithMessage(ErrTableScan, err.Error())
	}

	return nil
}

func (ts *TableScan) SetInt8(fieldName string, value int8) error {
	if err := ts.rp.SetInt8(ts.currentSlot, fieldName, value); err != nil {
		return errors.WithMessage(ErrTableScan, err.Error())
	}

	return nil
}

func (ts *TableScan) SetString(fieldName string, value string) error {
	if err := ts.rp.SetString(ts.currentSlot, fieldName, value); err != nil {
		return errors.WithMessage(ErrTableScan, err.Error())
	}

	return nil
}

func (ts *TableScan) SetVal(fieldName string, value Constant) error {
	switch t := ts.Layout.Schema.Type(fieldName); t {
	case records.Int64Field:
		v, ok := value.Value().(int64)
		if !ok {
			return errors.WithMessagef(ErrTableScan, "failed to convert fields (%s) constant to value (int64)", fieldName)
		}

		if err := ts.SetInt64(fieldName, v); err != nil {
			return err
		}
	case records.Int8Field:
		v, ok := value.Value().(int8)
		if !ok {
			return errors.WithMessagef(ErrTableScan, "failed to convert fields (%s) constant to value (int64)", fieldName)
		}

		if err := ts.SetInt8(fieldName, v); err != nil {
			return err
		}
	case records.StringField:
		v, ok := value.Value().(string)
		if !ok {
			return errors.WithMessagef(ErrTableScan, "failed to convert fields (%s) constant to value (int64)", fieldName)
		}

		if err := ts.SetString(fieldName, v); err != nil {
			return err
		}
	default:
		return errors.WithMessagef(ErrTableScan, "unknown field type %d for field '%s'", t, fieldName)
	}

	return nil
}

func (ts *TableScan) Insert() error {
	currentSlot, err := ts.rp.InsertAfter(ts.currentSlot)

	if err != nil && !errors.Is(err, records.ErrSlotNotFound) {
		return err
	}

	ts.currentSlot = currentSlot

	for ts.currentSlot < 0 {
		atLastBlock, err := ts.atLastBlock()
		if err != nil {
			return err
		}

		if atLastBlock {
			if err = ts.moveToNewBlock(); err != nil {
				return err
			}
		} else {
			if err = ts.moveToBlock(ts.rp.Block.Number + 1); err != nil {
				return err
			}
		}

		currentSlot, err := ts.rp.InsertAfter(ts.currentSlot)
		if err != nil && !errors.Is(err, records.ErrSlotNotFound) {
			return err
		}

		ts.currentSlot = currentSlot
	}

	return nil
}

func (ts *TableScan) Delete() error {
	if err := ts.rp.Delete(ts.currentSlot); err != nil {
		return err
	}

	return nil
}

func (ts *TableScan) MoveToRID(rid types.RID) error {
	ts.Close()

	block := types.Block{
		Filename: ts.Filename,
		Number:   rid.BlockNumber,
	}

	rp, err := records.NewRecordPage(ts.trx, block, ts.Layout)
	if err != nil {
		return errors.WithMessage(ErrTableScan, err.Error())
	}

	ts.rp = rp
	ts.currentSlot = rid.Slot

	return nil
}

func (ts *TableScan) RID() types.RID {
	rid := types.RID{}

	if ts.rp != nil {
		rid.BlockNumber = ts.rp.Block.Number
		rid.Slot = ts.currentSlot
	}

	return rid
}

func (ts *TableScan) moveToBlock(blockNumber types.BlockID) error {
	ts.Close()

	block := types.Block{
		Filename: ts.Filename,
		Number:   blockNumber,
	}

	rp, err := records.NewRecordPage(ts.trx, block, ts.Layout)
	if err != nil {
		return errors.WithMessage(ErrTableScan, err.Error())
	}

	ts.rp = rp
	ts.currentSlot = records.StartSlotID

	return nil
}

func (ts *TableScan) moveToNewBlock() error {
	ts.Close()

	block, err := ts.trx.Append(ts.Filename)
	if err != nil {
		return errors.WithMessage(ErrTableScan, err.Error())
	}

	rp, err := records.NewRecordPage(ts.trx, block, ts.Layout)
	if err != nil {
		return errors.WithMessage(ErrTableScan, err.Error())
	}

	_, err = rp.Format()
	if err != nil {
		return errors.WithMessage(ErrTableScan, err.Error())
	}

	ts.rp = rp
	ts.currentSlot = records.StartSlotID

	return nil
}

func (ts *TableScan) atLastBlock() (bool, error) {
	size, err := ts.trx.Size(ts.Filename)
	if err != nil {
		return false, errors.WithMessage(ErrTableScan, err.Error())
	}

	return ts.rp.Block.Number == size-1, nil
}