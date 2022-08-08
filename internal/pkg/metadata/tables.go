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

	if err := tcat.Insert(); err != nil {
		return t.wrapError(err, tableName, ErrFailedToCreateTable)
	}

	if err := tcat.ForEachField(func(name string, fieldType records.FieldType) (bool, error) {
		var err error

		switch name {
		case tcatTableNameField:
			err = tcat.SetString(tcatTableNameField, tableName)
		case tcatSlotsizeField:
			err = tcat.SetInt64(tcatSlotsizeField, int64(layout.SlotSize))
		}

		return false, err
	}); err != nil {
		return t.wrapError(err, tableName, ErrFailedToCreateTable)
	}

	for _, fieldName := range schema.Fields() {
		if err := fcat.Insert(); err != nil {
			return t.wrapError(err, tableName, ErrFailedToCreateTable)
		}

		if err := fcat.ForEachField(func(name string, fieldType records.FieldType) (bool, error) {
			var err error

			switch name {
			case fcatTableNameField:
				err = fcat.SetString(fcatTableNameField, tableName)
			case fcatFieldNameField:
				err = fcat.SetString(fcatFieldNameField, fieldName)
			case fcatTypeField:
				err = fcat.SetInt8(fcatTypeField, int8(schema.Type(fieldName)))
			case fcatLengthField:
				err = fcat.SetInt64(fcatLengthField, int64(schema.Length(fieldName)))
			case fcatOffsetField:
				err = fcat.SetInt64(fcatOffsetField, int64(layout.Offset(fieldName)))
			}

			if err != nil {
				return true, err
			}

			return false, err
		}); err != nil {
			return err
		}
	}

	return nil
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

	if err := tcat.ForEach(func() (bool, error) {
		tableInfo := struct {
			Name     string
			SlotSize int64
		}{}

		if err := tcat.ForEachValue(func(name string, fieldType records.FieldType, value interface{}) (bool, error) {
			var ok bool

			switch name {
			case tcatTableNameField:
				tableInfo.Name, ok = value.(string)
			case tcatSlotsizeField:
				tableInfo.SlotSize, ok = value.(int64)
			}

			if !ok {
				return true, errors.WithMessage(ErrTablesMetadata, records.ErrUnknownFieldType.Error())
			}

			return false, nil
		}); err != nil {
			return true, err
		}

		if tableInfo.Name != tableName {
			return false, nil
		}

		found = true

		layout.SlotSize = uint32(tableInfo.SlotSize)

		return false, nil
	}); err != nil {
		return layout, err
	}

	if !found {
		return layout, ErrTableNotFound
	}

	if err := fcat.ForEach(func() (bool, error) {
		fieldInfo := struct {
			TableName string
			FieldName string
			FieldType int8
			Length    int64
			Offset    int64
		}{}

		if err := fcat.ForEachValue(func(name string, fieldType records.FieldType, value interface{}) (bool, error) {
			var ok bool

			switch name {
			case fcatTableNameField:
				fieldInfo.TableName, ok = value.(string)
			case fcatFieldNameField:
				fieldInfo.FieldName, ok = value.(string)
			case fcatTypeField:
				fieldInfo.FieldType, ok = value.(int8)
			case fcatLengthField:
				fieldInfo.Length, ok = value.(int64)
			case fcatOffsetField:
				fieldInfo.Offset, ok = value.(int64)
			}

			if !ok {
				return true, errors.WithMessage(ErrTablesMetadata, records.ErrUnknownFieldType.Error())
			}

			return false, nil
		}); err != nil {
			return true, err
		}

		if fieldInfo.TableName != tableName {
			return false, nil
		}

		layout.Schema.AddField(fieldInfo.FieldName, records.FieldType(fieldInfo.FieldType), int(fieldInfo.Length))
		layout.Offsets[fieldInfo.FieldName] = uint32(fieldInfo.Offset)

		return false, nil
	}); err != nil {
		return layout, err
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
