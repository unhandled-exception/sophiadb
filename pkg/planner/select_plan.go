package planner

import (
	"fmt"

	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type SelectPlan struct {
	plan Plan
	pred scan.Predicate
}

func NewSelectPlan(plan Plan, pred scan.Predicate) (*SelectPlan, error) {
	p := &SelectPlan{
		plan: plan,
		pred: pred,
	}

	return p, nil
}

func (p *SelectPlan) Open() (scan.Scan, error) {
	s, err := p.plan.Open()
	if err != nil {
		return nil, err
	}

	return scan.NewSelectScan(s, p.pred), nil
}

func (p *SelectPlan) Schema() records.Schema {
	return p.plan.Schema()
}

func (p *SelectPlan) BlocksAccessed() int64 {
	return p.plan.BlocksAccessed()
}

func (p *SelectPlan) Records() int64 {
	return p.plan.Records()
}

func (p *SelectPlan) DistinctValues(fieldName string) (int64, bool) {
	if _, ok := p.pred.EquatesWithConstant(fieldName); ok {
		return 1, true
	}

	if otherFieldName, ok := p.pred.EquatesWithField(fieldName); ok {
		dv1, _ := p.plan.DistinctValues(fieldName)
		dv2, _ := p.plan.DistinctValues(otherFieldName)

		return max(dv1, dv2), true
	}

	return p.plan.DistinctValues(fieldName)
}

func (p *SelectPlan) String() string {
	pred := p.pred.String()
	if pred == "" {
		pred = "true"
	}

	return fmt.Sprintf("select from (%s) where %s", p.plan, pred)
}
