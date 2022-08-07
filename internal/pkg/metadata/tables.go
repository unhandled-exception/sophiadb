package metadata

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
)

const MaxNameLength = 32

const (
	defaultTcatTable = "sdb_tables"
	defaultFcatTable = "sbb_tables_fields"
)

const (
	tcatTableNameField = "tblname"
	tcatSlotsizeField  = "slotsize"

	fcatTableNameField = "tblname"
	fcatFieldNameField = "fldname"
	fcatTypeField      = "type"
	fcatLengthField    = "length"
	fcatOffsetField    = "offset"
)

type Tables struct {
	tcatTable string
	fcatTable string

	tcatLayout records.Layout
	fcatLayout records.Layout
}

func NewTables(isNew bool, trx records.TSTRXInt) (*Tables, error) {
	t := &Tables{
		tcatTable:  defaultTcatTable,
		fcatTable:  defaultFcatTable,
		tcatLayout: newTcatLayout(),
		fcatLayout: newFcatLayout(),
	}

	if isNew {
		if err := t.CreateTable(t.tcatTable, t.tcatLayout.Schema, trx); err != nil {
			return nil, err
		}

		if err := t.CreateTable(t.tcatTable, t.tcatLayout.Schema, trx); err != nil {
			return nil, err
		}
	}

	return t, nil
}

func newTcatLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddStringField(tcatTableNameField, MaxNameLength)
	schema.AddInt64Field(tcatSlotsizeField)

	return records.NewLayout(schema)
}

func newFcatLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddStringField(fcatTableNameField, MaxNameLength)
	schema.AddStringField(fcatFieldNameField, MaxNameLength)
	schema.AddInt8Field(fcatTypeField)
	schema.AddInt64Field(fcatLengthField)
	schema.AddInt64Field(fcatOffsetField)

	return records.NewLayout(schema)
}

func (t *Tables) CreateTable(tableName string, schema records.Schema, trx records.TSTRXInt) error {
	var err error

	tcat, err := records.NewTableScan(trx, t.tcatTable, t.tcatLayout)
	if err != nil {
		return t.wrapError(err, tableName, ErrFailedToCreateTable)
	}
	defer tcat.Close()

	fcat, err := records.NewTableScan(trx, t.fcatTable, t.fcatLayout)
	if err != nil {
		return t.wrapError(err, tableName, ErrFailedToCreateTable)
	}
	defer fcat.Close()

	layout := records.NewLayout(schema)

	if err = tcat.Insert(); err != nil {
		goto errs
	}

	if err = tcat.SetString(tcatTableNameField, tableName); err != nil {
		goto errs
	}

	if err = tcat.SetInt64(tcatSlotsizeField, int64(layout.SlotSize)); err != nil {
		goto errs
	}

	for _, fieldName := range schema.Fields() {
		if err = fcat.Insert(); err != nil {
			goto errs
		}

		if err = fcat.SetString(fcatTableNameField, tableName); err != nil {
			goto errs
		}

		if err = fcat.SetString(fcatFieldNameField, fieldName); err != nil {
			goto errs
		}

		if err = fcat.SetInt8(fcatTypeField, int8(schema.Type(fieldName))); err != nil {
			goto errs
		}

		if err = fcat.SetInt64(fcatLengthField, int64(schema.Length(fieldName))); err != nil {
			goto errs
		}

		if err = fcat.SetInt64(fcatOffsetField, int64(layout.Offset(fieldName))); err != nil {
			goto errs
		}
	}

	return nil

errs:
	return t.wrapError(err, tableName, ErrFailedToCreateTable)
}

func (t *Tables) Layout(tableName string, trx records.TSTRXInt) (records.Layout, error) {
	layout := records.Layout{
		Schema:  records.NewSchema(),
		Offsets: make(map[string]uint32, 16), //nolint:gomnd
	}

	tcat, err := records.NewTableScan(trx, t.tcatTable, t.tcatLayout)
	if err != nil {
		return layout, t.wrapError(err, tableName, nil)
	}
	defer tcat.Close()

	fcat, err := records.NewTableScan(trx, t.fcatTable, t.fcatLayout)
	if err != nil {
		return layout, t.wrapError(err, tableName, nil)
	}
	defer fcat.Close()

	found := false

	for {
		ok, terr := tcat.Next()
		if !ok {
			if terr != nil {
				return layout, t.wrapError(terr, tableName, nil)
			}

			break
		}

		tName, terr := tcat.GetString(tcatTableNameField)
		if terr != nil {
			return layout, t.wrapError(terr, tableName, nil)
		}

		if tName != tableName {
			continue
		}

		found = true

		tSlotSize, terr := tcat.GetInt64(tcatSlotsizeField)
		if terr != nil {
			return layout, t.wrapError(terr, tableName, nil)
		}

		layout.SlotSize = uint32(tSlotSize)

		break
	}

	if !found {
		return layout, ErrTableNotFound
	}

	for {
		ok, ferr := fcat.Next()
		if !ok {
			if ferr != nil {
				return layout, t.wrapError(ferr, tableName, nil)
			}

			break
		}

		tName, ferr := fcat.GetString(fcatTableNameField)
		if ferr != nil {
			return layout, t.wrapError(ferr, tableName, nil)
		}

		if tName != tableName {
			continue
		}

		fName, ferr := fcat.GetString(fcatFieldNameField)
		if ferr != nil {
			return layout, t.wrapError(ferr, tableName, nil)
		}

		fType, ferr := fcat.GetInt8(fcatTypeField)
		if ferr != nil {
			return layout, t.wrapError(ferr, tableName, nil)
		}

		fLength, ferr := fcat.GetInt64(fcatLengthField)
		if ferr != nil {
			return layout, t.wrapError(ferr, tableName, nil)
		}

		fOffset, ferr := fcat.GetInt64(fcatOffsetField)
		if ferr != nil {
			return layout, t.wrapError(ferr, tableName, nil)
		}

		layout.Schema.AddField(fName, records.FieldType(fType), int(fLength))
		layout.Offsets[fName] = uint32(fOffset)
	}

	if layout.Schema.Count() == 0 {
		return layout, ErrTableSchemaNotFound
	}

	return layout, nil
}

func (t *Tables) wrapError(err error, tableName string, baseError error) error {
	if baseError == nil {
		baseError = ErrTablesMetadata
	}

	return errors.WithMessagef(baseError, "table %s: %s", tableName, err)
}
