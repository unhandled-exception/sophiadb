package metadata

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type TablesManager interface {
	CreateTable(tableName string, schema records.Schema, trx scan.TRXInt) error
	Layout(tableName string, trx scan.TRXInt) (records.Layout, error)
}
