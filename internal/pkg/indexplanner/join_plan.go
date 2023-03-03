package indexplanner

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/planner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type jPlan interface {
	Open() (scan.Scan, error)
	Schema() records.Schema
	String() string
	DistinctValues(fieldName string) (int64, bool)
	Records() int64
	BlocksAccessed() int64
}

type jIndexInfo interface {
	Open() (indexes.Index, error)
	Records() int64
	BlocksAccessed() int64
	String() string
}

type JoinPlan struct {
	p1, p2    jPlan
	iiT2      jIndexInfo
	fieldName string
	schema    records.Schema
}

// NewJoinPlan создайт новый план сканирования для оператора объединения по индексу
func NewJoinPlan(p1, p2 planner.Plan, iiT2 jIndexInfo, fieldName string) (*JoinPlan, error) {
	p := &JoinPlan{
		p1:        p1,
		p2:        p2,
		iiT2:      iiT2,
		fieldName: fieldName,
		schema:    records.NewSchema(),
	}

	p.schema.AddAll(p1.Schema())
	p.schema.AddAll(p2.Schema())

	return p, nil
}

func (p *JoinPlan) Open() (scan.Scan, error) {
	lhs, err := p.p1.Open()
	if err != nil {
		return nil, errors.WithMessage(planner.ErrFailedToCreatePlan, err.Error())
	}

	sc, err := p.p2.Open()
	if err != nil {
		return nil, errors.WithMessage(planner.ErrFailedToCreatePlan, err.Error())
	}

	rhs, ok := sc.(*scan.TableScan)
	if !ok {
		return nil, errors.WithMessagef(planner.ErrFailedToCreatePlan, "wrapped plan return %T, required *scan.TableScan", rhs)
	}

	idx, err := p.iiT2.Open()
	if err != nil {
		return nil, errors.WithMessage(planner.ErrFailedToCreatePlan, err.Error())
	}

	return NewJoinScan(lhs, idx, p.fieldName, rhs)
}

func (p *JoinPlan) Schema() records.Schema {
	return p.schema
}

func (p *JoinPlan) BlocksAccessed() int64 {
	return p.p1.BlocksAccessed() +
		(p.p1.Records() * p.iiT2.BlocksAccessed()) +
		p.Records()
}

func (p *JoinPlan) Records() int64 {
	return p.p1.Records() * p.iiT2.Records()
}

func (p *JoinPlan) DistinctValues(field string) (int64, bool) {
	if dv, ok := p.p1.DistinctValues(field); ok {
		return dv, ok
	}

	return p.p2.DistinctValues(field)
}

func (p *JoinPlan) String() string {
	return fmt.Sprintf("join (%s) to (%s) on index (%q)", p.p1, p.p2, p.iiT2)
}
