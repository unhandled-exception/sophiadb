package metadata

import (
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type TablesManager interface {
	CreateTable(tableName string, schema records.Schema, trx scan.TRXInt) error
	Layout(tableName string, trx scan.TRXInt) (records.Layout, error)
}
