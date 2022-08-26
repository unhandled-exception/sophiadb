package metadata

import (
	"github.com/unhandled-exception/sophiadb/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type Manager struct {
	tables  *Tables
	views   *Views
	stats   *Stats
	indexes *Indexes
}

func NewManager(isNew bool, trx scan.TRXInt) (*Manager, error) {
	tables, err := NewTables(isNew, trx)
	if err != nil {
		return nil, err
	}

	views, err := NewViews(tables, isNew, trx)
	if err != nil {
		return nil, err
	}

	stats, err := NewStats(tables, trx)
	if err != nil {
		return nil, err
	}

	indexes, err := NewIndexes(tables, isNew, stats, trx)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		tables:  tables,
		views:   views,
		stats:   stats,
		indexes: indexes,
	}

	return m, nil
}

func (m *Manager) CreateTable(tableName string, schema records.Schema, trx scan.TRXInt) error {
	return m.tables.CreateTable(tableName, schema, trx)
}

func (m *Manager) Layout(tableName string, trx scan.TRXInt) (records.Layout, error) {
	return m.tables.Layout(tableName, trx)
}

func (m *Manager) ForEachTables(trx scan.TRXInt, call func(tableName string) (bool, error)) error {
	return m.tables.ForEachTables(trx, call)
}

func (m *Manager) CreateView(viewName string, viewDef string, trx scan.TRXInt) error {
	return m.views.CreateView(viewName, viewDef, trx)
}

func (m *Manager) ViewDef(viewName string, trx scan.TRXInt) (string, error) {
	return m.views.ViewDef(viewName, trx)
}

func (m *Manager) CreateIndex(idxName string, tableName string, idxType indexes.IndexType, fieldName string, trx scan.TRXInt) error {
	return m.indexes.CreateIndex(idxName, tableName, idxType, fieldName, trx)
}

func (m *Manager) TableIndexes(tableName string, trx scan.TRXInt) (IndexesMap, error) {
	return m.indexes.TableIndexes(tableName, trx)
}

func (m *Manager) GetStatInfo(tableName string, layout records.Layout, trx scan.TRXInt) (StatInfo, error) {
	return m.stats.GetStatInfo(tableName, layout, trx)
}
