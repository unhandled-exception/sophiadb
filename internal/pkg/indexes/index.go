package indexes

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type IndexType int8

const (
	HashIndexType  IndexType = 1
	BTreeIndexType IndexType = 2
)

var IndexTypeNames = map[IndexType]string{
	HashIndexType:  "hash",
	BTreeIndexType: "btree",
}

type Index interface {
	Type() IndexType
	Name() string
	Layout() records.Layout

	Close()
	SearchCost(blocks int64, recordsPerBlock int64) int64

	BeforeFirst(searchKey scan.Constant) error
	Next() (bool, error)
	RID() types.RID
	Insert(value scan.Constant, rid types.RID) error
	Delete(value scan.Constant, rid types.RID) error
}

const (
	IdxSchemaBlockField = "block"
	IdxSchemaIDField    = "id"
	IdxSchemaValueField = "dataval"
)

func New(trx scan.TRXInt, idxType IndexType, idxName string, layout records.Layout) (Index, error) {
	switch idxType {
	case HashIndexType:
		return NewStaticHashIndex(trx, idxName, layout)
	case BTreeIndexType:
		return NewBTreeIndex(trx, idxName, layout)
	}

	return nil, ErrUnknownIndexType
}

func NewIndexLayout(valueType records.FieldType, length int64) records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field(IdxSchemaBlockField)
	schema.AddInt64Field(IdxSchemaIDField)

	//nolint:exhaustive
	switch valueType {
	case records.Int64Field:
		schema.AddInt64Field(IdxSchemaValueField)
	case records.Int8Field:
		schema.AddInt8Field(IdxSchemaValueField)
	case records.StringField:
		schema.AddStringField(IdxSchemaValueField, length)
	}

	return records.NewLayout(schema)
}

func SearchCost(idxType IndexType, blocks int64, recordsPerBlock int64) int64 {
	switch idxType {
	case HashIndexType:
		return HashIndexSearchCost(blocks, recordsPerBlock)
	case BTreeIndexType:
		return BTreeIndexSearchCost(blocks, recordsPerBlock)
	}

	return -1
}
