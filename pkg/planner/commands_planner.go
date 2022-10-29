package planner

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type sqlCommandsPlannerMetadataManager interface {
	tablePlanMetadataManager

	CreateTable(tableName string, schema records.Schema, trx scan.TRXInt) error
	CreateIndex(idxName string, tableName string, idxType indexes.IndexType, fieldName string, trx scan.TRXInt) error
	CreateView(viewName string, viewDef string, trx scan.TRXInt) error
}

type SQLCommandsPlanner struct {
	mdm sqlCommandsPlannerMetadataManager
}

func NewSQLCommandsPlanner(mdm sqlCommandsPlannerMetadataManager) *SQLCommandsPlanner {
	p := &SQLCommandsPlanner{
		mdm: mdm,
	}

	return p
}

func (p *SQLCommandsPlanner) ExecuteInsert(stmt parse.InsertStatement, trx scan.TRXInt) (int64, error) {
	var (
		plan Plan
		err  error
	)

	plan, err = NewTablePlan(trx, stmt.TableName(), p.mdm)
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	sc, err := plan.Open()
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	defer sc.Close()

	us, ok := sc.(scan.UpdateScan)
	if !ok {
		return 0, errors.WithMessagef(ErrExecuteError, "failed to update (%s)", plan)
	}

	if len(stmt.Fields()) != len(stmt.Values()) {
		return 0, errors.WithMessage(ErrExecuteError, "insert fields count not equals values count")
	}

	values := stmt.Values()

	if werr := us.Insert(); werr != nil {
		return 0, errors.WithMessage(werr, werr.Error())
	}

	for i, field := range stmt.Fields() {
		if werr := us.SetVal(field, values[i]); werr != nil {
			return 0, errors.WithMessage(ErrExecuteError, werr.Error())
		}
	}

	return 1, nil
}

func (p *SQLCommandsPlanner) ExecuteDelete(stmt parse.DeleteStatement, trx scan.TRXInt) (int64, error) {
	var (
		plan Plan
		err  error
	)

	plan, err = NewTablePlan(trx, stmt.TableName(), p.mdm)
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	plan, err = NewSelectPlan(plan, stmt.Pred())
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	sc, err := plan.Open()
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	defer sc.Close()

	us, ok := sc.(scan.UpdateScan)
	if !ok {
		return 0, errors.WithMessagef(ErrExecuteError, "failed to update (%s)", plan)
	}

	rows := int64(0)

	if err = scan.ForEach(us, func() (bool, error) {
		if werr := us.Delete(); werr != nil {
			return true, werr
		}

		rows++

		return false, nil
	}); err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	return rows, nil
}

func (p *SQLCommandsPlanner) ExecuteUpdate(stmt parse.UpdateStatement, trx scan.TRXInt) (int64, error) {
	var (
		plan Plan
		err  error
	)

	plan, err = NewTablePlan(trx, stmt.TableName(), p.mdm)
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	plan, err = NewSelectPlan(plan, stmt.Pred())
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	sc, err := plan.Open()
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	defer sc.Close()

	us, ok := sc.(scan.UpdateScan)
	if !ok {
		return 0, errors.WithMessagef(ErrExecuteError, "failed to update (%s)", plan)
	}

	rows := int64(0)

	if err = scan.ForEach(us, func() (bool, error) {
		for _, expr := range stmt.UpdateExpressions() {
			if werr := us.SetVal(expr.FieldName, expr.Value); werr != nil {
				return true, werr
			}
		}

		rows++

		return false, nil
	}); err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	return rows, nil
}

func (p *SQLCommandsPlanner) ExecuteCreateTable(stmt parse.CreateTableStatement, trx scan.TRXInt) (int64, error) {
	if err := p.mdm.CreateTable(stmt.TableName(), stmt.Schema(), trx); err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	return 0, nil
}

func (p *SQLCommandsPlanner) ExecuteCreateIndex(stmt parse.CreateIndexStatement, trx scan.TRXInt) (int64, error) {
	if len(stmt.Fields()) > 1 {
		return 0, errors.WithMessage(ErrExecuteError, "composite keys isn't suported")
	}

	if err := p.mdm.CreateIndex(stmt.IndexName(), stmt.TableName(), stmt.IndexType(), stmt.Fields()[0], trx); err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	return 0, nil
}

func (p *SQLCommandsPlanner) ExecuteCreateView(stmt parse.CreateViewStatement, trx scan.TRXInt) (int64, error) {
	if err := p.mdm.CreateView(stmt.ViewName(), stmt.ViewDef(), trx); err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	return 0, nil
}
