package indexplanner

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type tablePlanMetadataManager interface {
	Layout(tableName string, trx scan.TRXInt) (records.Layout, error)
	GetStatInfo(tableName string, layout records.Layout, trx scan.TRXInt) (metadata.StatInfo, error)
	TableIndexes(tableName string, trx scan.TRXInt) (metadata.IndexesMap, error)
}

type sqlCommandsPlannerMetadataManager interface {
	tablePlanMetadataManager

	CreateTable(tableName string, schema records.Schema, trx scan.TRXInt) error
	CreateIndex(idxName string, tableName string, idxType indexes.IndexType, fieldName string, trx scan.TRXInt) error
	CreateView(viewName string, viewDef string, trx scan.TRXInt) error
}

type Plan interface {
	Open() (scan.Scan, error)
	Schema() records.Schema
	BlocksAccessed() int64
	Records() int64
	DistinctValues(string) (int64, bool)
	String() string
}
