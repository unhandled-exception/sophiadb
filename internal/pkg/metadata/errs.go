package metadata

import "github.com/pkg/errors"

var (
	ErrTablesMetadata      = errors.New("tables metadata error")
	ErrFailedToCreateTable = errors.New("failed to create table")
	ErrTableNotFound       = errors.Wrap(ErrTablesMetadata, "table not found")
	ErrTableSchemaNotFound = errors.Wrap(ErrTablesMetadata, "table schema not found")
)
