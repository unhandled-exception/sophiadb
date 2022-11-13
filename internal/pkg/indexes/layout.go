package indexes

import "github.com/unhandled-exception/sophiadb/internal/pkg/records"

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
