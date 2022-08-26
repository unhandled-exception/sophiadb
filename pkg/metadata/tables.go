package metadata

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

const (
	MaxTableNameLength = 32

	TableCatalogTableName  = "sdb_tables"
	fieldsCatalogTableName = "sbb_tables_fields"

	TcatTableNameField = "tblname"
	TcatSlotsizeField  = "slotsize"

	FcatTableNameField = "tblname"
	FcatFieldNameField = "fldname"
	FcatTypeField      = "type"
	FcatLengthField    = "length"
	FcatOffsetField    = "offset"
)

type Tables struct {
	TcatTable string
	FcatTable string

	TcatLayout records.Layout
	FcatLayout records.Layout
}

func NewTables(isNew bool, trx scan.TRXInt) (*Tables, error) {
	t := &Tables{
		TcatTable:  TableCatalogTableName,
		FcatTable:  fieldsCatalogTableName,
		TcatLayout: newTablesCatalogLayout(),
		FcatLayout: newFieldsCatalogLayout(),
	}

	if isNew {
		if err := t.CreateTable(t.TcatTable, t.TcatLayout.Schema, trx); err != nil {
			return nil, t.wrapError(err, "", nil)
		}

		if err := t.CreateTable(t.FcatTable, t.FcatLayout.Schema, trx); err != nil {
			return nil, t.wrapError(err, "", nil)
		}
	}

	return t, nil
}

func newTablesCatalogLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddStringField(TcatTableNameField, MaxTableNameLength)
	schema.AddInt64Field(TcatSlotsizeField)

	return records.NewLayout(schema)
}

func newFieldsCatalogLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddStringField(FcatTableNameField, MaxTableNameLength)
	schema.AddStringField(FcatFieldNameField, MaxTableNameLength)
	schema.AddInt8Field(FcatTypeField)
	schema.AddInt64Field(FcatLengthField)
	schema.AddInt64Field(FcatOffsetField)

	return records.NewLayout(schema)
}

func (t *Tables) TableExists(tableName string, trx scan.TRXInt) (bool, error) {
	tcat, err := t.NewTableCatalogTableScan(trx)
	if err != nil {
		return false, t.wrapError(err, tableName, nil)
	}

	found := false

	err = tcat.ForEach(func() (bool, error) {
		name, verr := tcat.GetString(TcatTableNameField)
		if verr != nil {
			return true, verr
		}

		found = (name == tableName)

		return found, nil
	})

	return found, t.wrapError(err, tableName, nil)
}

func (t *Tables) CreateTable(tableName string, schema records.Schema, trx scan.TRXInt) error {
	tableExists, err := t.TableExists(tableName, trx)
	if tableExists || err != nil {
		if err != nil {
			return t.wrapError(err, tableName, ErrFailedToCreateTable)
		}

		if tableExists {
			return ErrTableExists
		}
	}

	tcat, fcat, err := t.NewCatalogTableScan(trx)
	if err != nil {
		return t.wrapError(err, tableName, ErrFailedToCreateTable)
	}
	defer tcat.Close()
	defer fcat.Close()

	layout := records.NewLayout(schema)

	if err := tcat.Insert(); err != nil {
		return t.wrapError(err, tableName, ErrFailedToCreateTable)
	}

	if err := tcat.ForEachField(func(name string, fieldType records.FieldType) (bool, error) {
		var err error

		switch name {
		case TcatTableNameField:
			err = tcat.SetString(TcatTableNameField, tableName)
		case TcatSlotsizeField:
			err = tcat.SetInt64(TcatSlotsizeField, int64(layout.SlotSize))
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
			case FcatTableNameField:
				err = fcat.SetString(FcatTableNameField, tableName)
			case FcatFieldNameField:
				err = fcat.SetString(FcatFieldNameField, fieldName)
			case FcatTypeField:
				err = fcat.SetInt8(FcatTypeField, int8(schema.Type(fieldName)))
			case FcatLengthField:
				err = fcat.SetInt64(FcatLengthField, int64(schema.Length(fieldName)))
			case FcatOffsetField:
				err = fcat.SetInt64(FcatOffsetField, int64(layout.Offset(fieldName)))
			}

			return false, err
		}); err != nil {
			return err
		}
	}

	return nil
}

func (t *Tables) Layout(tableName string, trx scan.TRXInt) (records.Layout, error) {
	layout := records.Layout{
		Schema:  records.NewSchema(),
		Offsets: make(map[string]uint32, 16), //nolint:gomnd
	}

	tcat, fcat, err := t.NewCatalogTableScan(trx)
	if err != nil {
		return layout, t.wrapError(err, tableName, ErrFailedToCreateTable)
	}
	defer tcat.Close()
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
			case TcatTableNameField:
				tableInfo.Name, ok = value.(string)
			case TcatSlotsizeField:
				tableInfo.SlotSize, ok = value.(int64)
			}

			if !ok {
				return true, errors.WithMessage(ErrTablesMetadata, scan.ErrUnknownFieldType.Error())
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
			case FcatTableNameField:
				fieldInfo.TableName, ok = value.(string)
			case FcatFieldNameField:
				fieldInfo.FieldName, ok = value.(string)
			case FcatTypeField:
				fieldInfo.FieldType, ok = value.(int8)
			case FcatLengthField:
				fieldInfo.Length, ok = value.(int64)
			case FcatOffsetField:
				fieldInfo.Offset, ok = value.(int64)
			}

			if !ok {
				return true, errors.WithMessage(ErrTablesMetadata, scan.ErrUnknownFieldType.Error())
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

func (t *Tables) NewTableCatalogTableScan(trx scan.TRXInt) (*scan.TableScan, error) {
	return scan.NewTableScan(trx, t.TcatTable, t.TcatLayout)
}

func (t *Tables) NewFieldsCatalogTableScan(trx scan.TRXInt) (*scan.TableScan, error) {
	return scan.NewTableScan(trx, t.FcatTable, t.FcatLayout)
}

func (t *Tables) NewCatalogTableScan(trx scan.TRXInt) (*scan.TableScan /* tcat */, *scan.TableScan /* fcat */, error) {
	tcat, err := t.NewTableCatalogTableScan(trx)
	if err != nil {
		return nil, nil, err
	}

	fcat, err := t.NewFieldsCatalogTableScan(trx)
	if err != nil {
		return nil, nil, err
	}

	return tcat, fcat, err
}

func (t *Tables) ForEachTables(trx scan.TRXInt, call func(tableName string) (bool, error)) error {
	ts, err := t.NewTableCatalogTableScan(trx)
	if err != nil {
		return err
	}

	defer ts.Close()

	err = ts.ForEach(func() (bool, error) {
		tableName, werr := ts.GetString(TcatTableNameField)
		if werr != nil {
			return true, t.wrapError(werr, t.TcatTable, nil)
		}

		return call(tableName)
	})

	return err
}

func (t *Tables) wrapError(err error, tableName string, baseError error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, ErrTablesMetadata) {
		return err
	}

	if baseError == nil {
		baseError = ErrTablesMetadata
	}

	return errors.WithMessagef(baseError, "table %s: %s", tableName, err)
}
