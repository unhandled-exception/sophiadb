package indexes

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

const (
	BTreeNewFlag = -1
)

type BTreeDirEntry struct {
	BlockNumber types.BlockID
	Dataval     scan.Constant
}

func NewBTreeDirPageLayout(datavalFieldType records.FieldType, length int64) records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field(blockFieldName)

	//nolint:exhaustive
	switch datavalFieldType {
	case records.Int64Field:
		schema.AddInt64Field(datavalFieldName)
	case records.Int8Field:
		schema.AddInt8Field(datavalFieldName)
	case records.StringField:
		schema.AddStringField(datavalFieldName, length)
	}

	layout := records.NewLayout(schema)

	return layout
}

func NewBTreeLeafPageLayout(datavalFieldType records.FieldType, length int64) records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field(idFieldName)
	schema.AddInt64Field(blockFieldName)

	//nolint:exhaustive
	switch datavalFieldType {
	case records.Int64Field:
		schema.AddInt64Field(datavalFieldName)
	case records.Int8Field:
		schema.AddInt8Field(datavalFieldName)
	case records.StringField:
		schema.AddStringField(datavalFieldName, length)
	}

	layout := records.NewLayout(schema)

	return layout
}
