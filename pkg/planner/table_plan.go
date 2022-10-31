package planner

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type TablePlan struct {
	trx       scan.TRXInt
	tablename string
	layout    records.Layout
	stats     metadata.StatInfo
}

type tablePlanMetadataManager interface {
	Layout(tableName string, trx scan.TRXInt) (records.Layout, error)
	GetStatInfo(tableName string, layout records.Layout, trx scan.TRXInt) (metadata.StatInfo, error)
}

func NewTablePlan(trx scan.TRXInt, tableName string, md tablePlanMetadataManager) (*TablePlan, error) {
	p := &TablePlan{
		trx:       trx,
		tablename: tableName,
	}

	var err error

	p.layout, err = md.Layout(tableName, trx)
	if err != nil {
		return nil, errors.WithMessage(ErrFailedToCreatePlan, err.Error())
	}

	p.stats, err = md.GetStatInfo(tableName, p.layout, trx)
	if err != nil {
		return nil, errors.WithMessage(ErrFailedToCreatePlan, err.Error())
	}

	return p, nil
}

func (p *TablePlan) Open() (scan.Scan, error) {
	return scan.NewTableScan(p.trx, p.tablename, p.layout)
}

func (p *TablePlan) Schema() records.Schema {
	return p.layout.Schema
}

func (p *TablePlan) BlocksAccessed() int64 {
	return p.stats.Blocks
}

func (p *TablePlan) Records() int64 {
	return p.stats.Records
}

func (p *TablePlan) DistinctValues(fieldName string) (int64, bool) {
	return p.stats.DistinctValues(fieldName)
}

func (p *TablePlan) String() string {
	return fmt.Sprintf("scan table %s", p.tablename)
}
