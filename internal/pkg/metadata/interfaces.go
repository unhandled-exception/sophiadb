package metadata

import "github.com/unhandled-exception/sophiadb/internal/pkg/records"

type TablesManager interface {
	CreateTable(tableName string, schema records.Schema, trx records.TSTRXInt) error
	Layout(tableName string, trx records.TSTRXInt) (records.Layout, error)
}
