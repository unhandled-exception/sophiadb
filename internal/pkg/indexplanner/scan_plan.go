package indexplanner

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/planner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type sPlan interface {
	Open() (scan.Scan, error)
	Schema() records.Schema
	String() string
}

type sIndexInfo interface {
	Open() (indexes.Index, error)
	String() string
	DistinctValues(fieldName string) int64
	Records() int64
	BlocksAccessed() int64
}

// SelectPlan — план сканирования для поиска по индексу
type SelectPlan struct {
	p     sPlan
	ii    sIndexInfo
	value scan.Constant
}

// NewSelectPlan создаёт новый план сканирования по индексу
func NewSelectPlan(p sPlan, ii sIndexInfo, value scan.Constant) (*SelectPlan, error) {
	ip := &SelectPlan{
		p:     p,
		ii:    ii,
		value: value,
	}

	return ip, nil
}

func (ip *SelectPlan) Open() (scan.Scan, error) {
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

	return NewSelectScan(ts, idx, ip.value)
}

func (ip *SelectPlan) Schema() records.Schema {
	return ip.p.Schema()
}

func (ip *SelectPlan) BlocksAccessed() int64 {
	return ip.ii.BlocksAccessed() + ip.ii.Records()
}

func (ip *SelectPlan) Records() int64 {
	return ip.ii.Records()
}

func (ip *SelectPlan) DistinctValues(fieldName string) (int64, bool) {
	return ip.ii.DistinctValues(fieldName), true
}

func (ip *SelectPlan) String() string {
	return fmt.Sprintf("index scan on %q", ip.ii)
}
