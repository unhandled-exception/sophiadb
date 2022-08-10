package metadata

import "github.com/pkg/errors"

var (
	ErrTablesMetadata      = errors.New("tables metadata error")
	ErrFailedToCreateTable = errors.New("failed to create table")
	ErrTableNotFound       = errors.Wrap(ErrTablesMetadata, "table not found")
	ErrTableExists         = errors.Wrap(ErrTablesMetadata, "table already exists")
	ErrTableSchemaNotFound = errors.Wrap(ErrTablesMetadata, "table schema not found")

	ErrViewsMetadata = errors.New("views metadata error")
	ErrViewNotFound  = errors.Wrap(ErrViewsMetadata, "view not found")
	ErrViewExists    = errors.Wrap(ErrViewsMetadata, "view already exists")
)
