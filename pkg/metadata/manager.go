package metadata

import (
	"github.com/unhandled-exception/sophiadb/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/pkg/records"
)

type Manager struct {
	tables  *Tables
	views   *Views
	stats   *Stats
	indexes *Indexes
}

func NewManager(isNew bool, trx records.TSTRXInt) (*Manager, error) {
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

func (m *Manager) CreateTable(tableName string, schema records.Schema, trx records.TSTRXInt) error {
	return m.tables.CreateTable(tableName, schema, trx)
}

func (m *Manager) Layout(tableName string, trx records.TSTRXInt) (records.Layout, error) {
	return m.tables.Layout(tableName, trx)
}

func (m *Manager) ForEachTables(trx records.TSTRXInt, call func(tableName string) (bool, error)) error {
	return m.tables.ForEachTables(trx, call)
}

func (m *Manager) CreateView(viewName string, viewDef string, trx records.TSTRXInt) error {
	return m.views.CreateView(viewName, viewDef, trx)
}

func (m *Manager) ViewDef(viewName string, trx records.TSTRXInt) (string, error) {
	return m.views.ViewDef(viewName, trx)
}

func (m *Manager) CreateIndex(idxName string, tableName string, idxType indexes.IndexType, fieldName string, trx records.TSTRXInt) error {
	return m.indexes.CreateIndex(idxName, tableName, idxType, fieldName, trx)
}

func (m *Manager) TableIndexes(tableName string, trx records.TSTRXInt) (IndexesMap, error) {
	return m.indexes.TableIndexes(tableName, trx)
}

func (m *Manager) GetStatInfo(tableName string, layout records.Layout, trx records.TSTRXInt) (StatInfo, error) {
	return m.stats.GetStatInfo(tableName, layout, trx)
}
