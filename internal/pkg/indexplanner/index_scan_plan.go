package indexplanner

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/planner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type plan interface {
	Open() (scan.Scan, error)
	Schema() records.Schema
	String() string
}

type indexInfo interface {
	Open() (indexes.Index, error)
	DistinctValues(fieldName string) int64
	Records() int64
	BlocksAccessed() int64
	String() string
}

// IndexPlan — образ сканирования для поиска по индексу
type IndexPlan struct {
	p     plan
	ii    indexInfo
	value scan.Constant
}

// NewIndexPlan возвращает новый образ сканирования по индексу
func NewIndexPlan(p plan, ii indexInfo, value scan.Constant) (*IndexPlan, error) {
	ip := &IndexPlan{
		p:     p,
		ii:    ii,
		value: value,
	}

	return ip, nil
}

func (ip *IndexPlan) Open() (scan.Scan, error) {
	sc, err := ip.p.Open()
	if err != nil {
		return nil, errors.WithMessage(planner.ErrFailedToCreatePlan, err.Error())
	}

	ts, ok := sc.(*scan.TableScan)
	if !ok {
		return nil, errors.WithMessagef(planner.ErrFailedToCreatePlan, "wrapped plan return %T, required *scan.TableScan", ts)
	}

	idx, err := ip.ii.Open()
	if err != nil {
		return nil, errors.WithMessage(planner.ErrFailedToCreatePlan, err.Error())
	}

	return NewIndexSelectScan(ts, idx, ip.value)
}

func (ip *IndexPlan) Schema() records.Schema {
	return ip.p.Schema()
}

func (ip *IndexPlan) BlocksAccessed() int64 {
	return ip.ii.BlocksAccessed() + ip.ii.Records()
}

func (ip *IndexPlan) Records() int64 {
	return ip.ii.Records()
}

func (ip *IndexPlan) DistinctValues(fieldName string) (int64, bool) {
	return ip.ii.DistinctValues(fieldName), true
}

func (ip *IndexPlan) String() string {
	return fmt.Sprintf("index scan on %q", ip.ii.String())
}
