package planner

import (
	"fmt"
	"strings"

	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type ProjectPlan struct {
	plan   Plan
	schema records.Schema
}

func NewProjectPlan(plan Plan, fields ...string) (*ProjectPlan, error) {
	p := &ProjectPlan{
		plan:   plan,
		schema: records.NewSchema(),
	}

	parentSchema := plan.Schema()

	for _, field := range fields {
		fi, ok := parentSchema.Field(field)
		if !ok {
			continue
		}

		p.schema.AddField(field, fi.Type, fi.Length)
	}

	return p, nil
}

func (p *ProjectPlan) Open() (scan.Scan, error) {
	s, err := p.plan.Open()
	if err != nil {
		return nil, err
	}

	return scan.NewProjectScan(s, p.schema.Fields()...), nil
}

func (p *ProjectPlan) Schema() records.Schema {
	return p.schema
}

func (p *ProjectPlan) BlocksAccessed() int64 {
	return p.plan.BlocksAccessed()
}

func (p *ProjectPlan) Records() int64 {
	return p.plan.Records()
}

func (p *ProjectPlan) DistinctValues(fieldName string) (int64, bool) {
	return p.plan.DistinctValues(fieldName)
}

func (p *ProjectPlan) String() string {
	return fmt.Sprintf(
		"choose %s from (%s)",
		strings.Join(p.schema.Fields(), ", "),
		p.plan,
	)
}
