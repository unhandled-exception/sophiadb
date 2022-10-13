package metadata

import (
	"fmt"

	"github.com/unhandled-exception/sophiadb/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

const (
	IdxSchemaBlockField = "block"
	IdxSchemaIDField    = "id"
	IdxSchemaValueField = "dataval"
)

type IndexInfo struct {
	idxName   string
	tableName string
	idxType   indexes.IndexType
	fieldName string
	trx       scan.TRXInt
	schema    records.Schema
	idxLayout records.Layout
	si        StatInfo
}

func NewIndexInfo(idxName string, tableName string, idxType indexes.IndexType, fieldName string, schema records.Schema, trx scan.TRXInt, si StatInfo) *IndexInfo {
	ii := &IndexInfo{
		idxName:   idxName,
		tableName: tableName,
		idxType:   idxType,
		fieldName: fieldName,
		trx:       trx,
		schema:    schema,
		si:        si,
	}

	ii.idxLayout = ii.createIndexLayout()

	return ii
}

func (ii *IndexInfo) String() string {
	indexTypeName := "unknown"

	switch ii.idxType {
	case indexes.HashIndexType:
		indexTypeName = "hash"
	case indexes.BTreeIndexType:
		indexTypeName = "btree"
	}

	return fmt.Sprintf(
		`"%s" on "%s.%s" [%s blocks: %d, records %d, distinct values: %d]`,
		ii.idxName,
		ii.tableName,
		ii.fieldName,
		indexTypeName,
		ii.BlocksAccessed(),
		ii.Records(),
		ii.DistinctValues(ii.fieldName),
	)
}

func (ii *IndexInfo) Open() (indexes.Index, error) {
	switch ii.idxType {
	case indexes.HashIndexType:
		return indexes.NewHashIndex(ii.trx, ii.idxName, ii.idxLayout)
	case indexes.BTreeIndexType:
		return indexes.NewBTreeIndex(ii.trx, ii.idxName, ii.idxLayout)
	}

	return nil, ErrUnknownIndexType
}

func (ii *IndexInfo) BlocksAccessed() int64 {
	recordsPerBlock := int64(ii.trx.BlockSize() / ii.idxLayout.SlotSize)
	blocks := ii.si.Records / recordsPerBlock

	switch ii.idxType {
	case indexes.HashIndexType:
		return indexes.HashIndexSearchCost(blocks, recordsPerBlock)
	case indexes.BTreeIndexType:
		return indexes.BTreeIndexSearchCost(blocks, recordsPerBlock)
	}

	return -1
}

func (ii *IndexInfo) Records() int64 {
	dv, _ := ii.si.DistinctValues(ii.fieldName)
	if dv == 0 {
		return 0
	}

	return ii.si.Records / dv
}

func (ii *IndexInfo) DistinctValues(fieldName string) int64 {
	if fieldName != ii.fieldName {
		return 1
	}

	dv, _ := ii.si.DistinctValues(ii.fieldName)

	return dv
}

func (ii *IndexInfo) createIndexLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field(IdxSchemaBlockField)
	schema.AddInt64Field(IdxSchemaIDField)

	//nolint:exhaustive
	switch ii.schema.Type(ii.fieldName) {
	case records.Int64Field:
		schema.AddInt64Field(IdxSchemaValueField)
	case records.Int8Field:
		schema.AddInt8Field(IdxSchemaValueField)
	case records.StringField:
		schema.AddStringField(IdxSchemaValueField, ii.schema.Length(ii.fieldName))
	}

	return records.NewLayout(schema)
}
