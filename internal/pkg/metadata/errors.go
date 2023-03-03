package metadata

import "github.com/pkg/errors"

var (
	ErrMetadata               = errors.New("metadata error")
	ErrTablesMetadata         = errors.Wrap(ErrMetadata, "tables error")
	ErrFailedToCreateTable    = errors.Wrap(ErrMetadata, "failed to create table")
	ErrTableNotFound          = errors.Wrap(ErrTablesMetadata, "table not found")
	ErrTableExists            = errors.Wrap(ErrTablesMetadata, "table already exists")
	ErrTableSchemaNotFound    = errors.Wrap(ErrTablesMetadata, "table schema not found")
	ErrViewsMetadata          = errors.Wrap(ErrMetadata, "views error")
	ErrViewNotFound           = errors.Wrap(ErrViewsMetadata, "view not found")
	ErrViewExists             = errors.Wrap(ErrViewsMetadata, "view already exists")
	ErrStatsMetadata          = errors.Wrap(ErrMetadata, "stats error")
	ErrsStatsUnknownFieldType = errors.Wrap(ErrStatsMetadata, "unknown field type")
	ErrIndexesMetadata        = errors.Wrap(ErrMetadata, "indexes error")
	ErrIndexExists            = errors.Wrap(ErrIndexesMetadata, "index already exists")
	ErrFieldIndexed           = errors.Wrap(ErrIndexesMetadata, "field already indexed")
	ErrFailedToOpenIndex      = errors.Wrap(ErrIndexesMetadata, "failed to open index")
)
