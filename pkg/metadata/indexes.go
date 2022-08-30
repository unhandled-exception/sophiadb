package metadata

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

const (
	IndexCatalogTableName = "sdb_indexes"

	IcatIndexTypeField = "indextype"
	IcatIndexNameField = "indexname"
	IcatTableNameField = "tablename"
	IcatFieldNameField = "fieldname"
)

type IndexesMap map[string]*IndexInfo

type Indexes struct {
	layout       records.Layout
	tables       *Tables
	stats        *Stats
	catTableName string
}

func NewIndexes(tables *Tables, isNew bool, stats *Stats, trx scan.TRXInt) (*Indexes, error) {
	i := &Indexes{
		layout:       newIndexCatalogLayout(),
		tables:       tables,
		stats:        stats,
		catTableName: IndexCatalogTableName,
	}

	if isNew {
		if err := tables.CreateTable(i.catTableName, i.layout.Schema, trx); err != nil {
			return nil, i.wrapError(err, i.catTableName, nil)
		}
	}

	return i, nil
}

func newIndexCatalogLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddInt8Field(IcatIndexTypeField)
	schema.AddStringField(IcatIndexNameField, MaxTableNameLength)
	schema.AddStringField(IcatTableNameField, MaxTableNameLength)
	schema.AddStringField(IcatFieldNameField, MaxTableNameLength)

	return records.NewLayout(schema)
}

func (i *Indexes) NewIndexCatalogTableScan(trx scan.TRXInt) (*scan.TableScan, error) {
	ts, err := scan.NewTableScan(trx, i.catTableName, i.layout)
	if err != nil {
		return nil, i.wrapError(err, i.catTableName, nil)
	}

	return ts, nil
}

func (i *Indexes) CreateIndex(idxName string, tableName string, idxType indexes.IndexType, fieldName string, trx scan.TRXInt) error {
	ts, err := i.NewIndexCatalogTableScan(trx)
	if err != nil {
		return err
	}

	defer ts.Close()

	if err = scan.ForEach(ts, func() (bool, error) {
		iName, verr := ts.GetString(IcatIndexNameField)
		switch {
		case verr != nil:
			return true, verr
		case iName == idxName:
			return true, ErrIndexExists
		}

		tName, verr := ts.GetString(IcatTableNameField)
		if verr != nil {
			return true, verr
		}

		if tName != tableName {
			return false, nil
		}

		fName, verr := ts.GetString(IcatFieldNameField)
		switch {
		case verr != nil:
			return true, verr
		case fName == fieldName:
			return true, ErrFieldIndexed
		}

		return false, nil
	}); err != nil {
		return i.wrapError(err, tableName, nil)
	}

	if err = ts.Insert(); err != nil {
		return i.wrapError(err, tableName, nil)
	}

	err = scan.ForEachField(ts, func(name string, fieldType records.FieldType) (bool, error) {
		var verr error

		switch name {
		case IcatIndexTypeField:
			verr = ts.SetInt8(IcatIndexTypeField, int8(idxType))
		case IcatIndexNameField:
			verr = ts.SetString(IcatIndexNameField, idxName)
		case IcatTableNameField:
			verr = ts.SetString(IcatTableNameField, tableName)
		case IcatFieldNameField:
			verr = ts.SetString(IcatFieldNameField, fieldName)
		}

		return false, verr
	})

	return i.wrapError(err, tableName, nil)
}

func (i *Indexes) TableIndexes(tableName string, trx scan.TRXInt) (IndexesMap, error) {
	layout, err := i.tables.Layout(tableName, trx)
	if err != nil {
		return nil, i.wrapError(err, tableName, nil)
	}

	si, err := i.stats.GetStatInfo(tableName, layout, trx)
	if err != nil {
		return nil, i.wrapError(err, tableName, nil)
	}

	ts, err := i.NewIndexCatalogTableScan(trx)
	if err != nil {
		return nil, err
	}

	defer ts.Close()

	imap := make(IndexesMap)

	err = scan.ForEach(ts, func() (bool, error) {
		tName, verr := ts.GetString(IcatTableNameField)
		if verr != nil {
			return true, err
		}

		if tName != tableName {
			return false, nil
		}

		var (
			iType int8
			iName string
			fName string
		)

		verr = scan.ForEachField(ts, func(name string, fieldType records.FieldType) (bool, error) {
			var werr error

			switch name {
			case IcatIndexTypeField:
				iType, werr = ts.GetInt8(IcatIndexTypeField)
			case IcatIndexNameField:
				iName, werr = ts.GetString(IcatIndexNameField)
			case IcatFieldNameField:
				fName, werr = ts.GetString(IcatFieldNameField)
			}

			return false, werr
		})

		if verr == nil {
			imap[fName] = NewIndexInfo(iName, tName, indexes.IndexType(iType), fName, layout.Schema, trx, si)
		}

		return false, verr
	})

	return imap, i.wrapError(err, tableName, nil)
}

func (i *Indexes) wrapError(err error, tableName string, baseError error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, ErrIndexesMetadata) {
		return err
	}

	if baseError == nil {
		baseError = ErrIndexesMetadata
	}

	return errors.WithMessagef(baseError, "table %s: %s", tableName, err)
}
