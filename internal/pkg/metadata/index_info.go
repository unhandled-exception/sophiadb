package metadata

import (
	"fmt"

	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
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
	return fmt.Sprintf(
		`"%s" on "%s.%s" using %s [blocks: %d, records %d, distinct values: %d]`,
		ii.idxName,
		ii.tableName,
		ii.fieldName,
		indexes.IndexTypeNames[ii.idxType],
		ii.BlocksAccessed(),
		ii.Records(),
		ii.DistinctValues(ii.fieldName),
	)
}

func (ii *IndexInfo) Open() (indexes.Index, error) {
	switch ii.idxType {
	case indexes.HashIndexType:
		return indexes.NewStaticHashIndex(ii.trx, ii.idxName, ii.idxLayout)
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
	return indexes.NewIndexLayout(ii.schema.Type(ii.fieldName), ii.schema.Length(ii.fieldName))
}
