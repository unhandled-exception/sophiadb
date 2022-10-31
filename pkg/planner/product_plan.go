package planner

import (
	"fmt"

	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type ProductPlan struct {
	p1     Plan
	p2     Plan
	schema records.Schema
}

func NewProductPlan(p1 Plan, p2 Plan) (*ProductPlan, error) {
	p := &ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: records.NewSchema(),
	}

	p.schema.AddAll(p1.Schema())
	p.schema.AddAll(p2.Schema())

	return p, nil
}

func (p *ProductPlan) Open() (scan.Scan, error) {
	s1, err := p.p1.Open()
	if err != nil {
		return nil, err
	}

	s2, err := p.p2.Open()
	if err != nil {
		return nil, err
	}

	return scan.NewProductScan(s1, s2), nil
}

func (p *ProductPlan) Schema() records.Schema {
	return p.schema
}

func (p *ProductPlan) BlocksAccessed() int64 {
	return p.p1.BlocksAccessed() +
		(p.p1.Records() * p.p2.BlocksAccessed())
}

func (p *ProductPlan) Records() int64 {
	return p.p1.Records() * p.p2.Records()
}

func (p *ProductPlan) DistinctValues(fieldName string) (int64, bool) {
	if v, ok := p.p1.DistinctValues(fieldName); ok {
		return v, true
	}

	return p.p2.DistinctValues(fieldName)
}

func (p *ProductPlan) String() string {
	return fmt.Sprintf("join (%s) to (%s)", p.p1, p.p2)
}
