package planner

import (
	"errors"

	"github.com/unhandled-exception/sophiadb/internal/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/internal/pkg/parse"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type sqlQueryPlannerMetadataManager interface {
	tablePlanMetadataManager

	ViewDef(viewName string, trx scan.TRXInt) (string, error)
}

type SQLQueryPlanner struct {
	mdm sqlQueryPlannerMetadataManager
}

func NewSQLQueryPlanner(mdm sqlQueryPlannerMetadataManager) *SQLQueryPlanner {
	p := &SQLQueryPlanner{
		mdm: mdm,
	}

	return p
}

func (p *SQLQueryPlanner) CreatePlan(stmt parse.SelectStatement, trx scan.TRXInt) (Plan, error) {
	plan, err := p.makeTablesPlan(stmt, trx)
	if err != nil {
		return nil, err
	}

	plan, err = NewSelectPlan(plan, stmt.Pred())
	if err != nil {
		return nil, err
	}

	plan, err = NewProjectPlan(plan, stmt.Fields()...)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func (p *SQLQueryPlanner) makeTablesPlan(stmt parse.SelectStatement, trx scan.TRXInt) (Plan, error) {
	plans := make([]Plan, len(stmt.Tables()))

	for i, table := range stmt.Tables() {
		switch viewDef, err := p.mdm.ViewDef(table, trx); {
		case errors.Is(err, metadata.ErrViewNotFound):
		case err != nil:
			return nil, err
		default:
			vp, err := p.makeViewPlan(viewDef, trx)
			if err != nil {
				return nil, err
			}

			plans[i] = vp

			continue
		}

		var err error

		plans[i], err = NewTablePlan(trx, table, p.mdm)
		if err != nil {
			return nil, err
		}
	}

	var err error

	plan := plans[0]
	for _, nextPlan := range plans[1:] {
		if plan, err = NewProductPlan(plan, nextPlan); err != nil {
			return nil, err
		}
	}

	return plan, nil
}

func (p *SQLQueryPlanner) makeViewPlan(viewDef string, trx scan.TRXInt) (Plan, error) {
	stmtType, stmt, err := parse.ParseQuery(viewDef)

	switch {
	case stmtType != parse.StmtSelect:
		return nil, parse.ErrBadSyntax
	case err != nil:
		return nil, err
	}

	return p.CreatePlan(stmt.(parse.SelectStatement), trx) //nolint:forcetypeassert
}
