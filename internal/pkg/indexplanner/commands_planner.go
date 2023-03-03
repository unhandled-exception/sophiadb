package indexplanner

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/parse"
	"github.com/unhandled-exception/sophiadb/internal/pkg/planner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type IndexCommandsPlanner struct {
	mdm sqlCommandsPlannerMetadataManager
}

func NewIndexCommandsPlanner(mdm sqlCommandsPlannerMetadataManager) *IndexCommandsPlanner {
	p := &IndexCommandsPlanner{
		mdm: mdm,
	}

	return p
}

func (p *IndexCommandsPlanner) ExecuteInsert(stmt parse.InsertStatement, trx scan.TRXInt) (int64, error) {
	var (
		plan Plan
		err  error
	)

	plan, err = planner.NewTablePlan(trx, stmt.TableName(), p.mdm)
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	sc, err := plan.Open()
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	defer sc.Close()

	indexes, err := p.mdm.TableIndexes(stmt.TableName(), trx)
	if err != nil {
		return 0, errors.WithMessagef(ErrExecuteError, "failed to get tables indexes (%s): %q", plan, err)
	}

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

		idxInfo, ok := indexes[field]
		if ok {
			idx, err := idxInfo.Open()
			if err != nil {
				return 0, errors.WithMessagef(ErrExecuteError, "failed to open index (%s): %q", plan, err)
			}

			defer idx.Close()

			if err = idx.Insert(values[i], us.RID()); err != nil {
				return 0, errors.WithMessagef(ErrExecuteError, "failed to open index (%s): %q", plan, err)
			}

		}
	}

	return 1, nil
}

func (p *IndexCommandsPlanner) ExecuteDelete(stmt parse.DeleteStatement, trx scan.TRXInt) (int64, error) {
	var (
		plan Plan
		err  error
	)

	plan, err = planner.NewTablePlan(trx, stmt.TableName(), p.mdm)
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	plan, err = planner.NewSelectPlan(plan, stmt.Pred())
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	sc, err := plan.Open()
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	defer sc.Close()

	indexes, err := p.mdm.TableIndexes(stmt.TableName(), trx)
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	us, ok := sc.(scan.UpdateScan)
	if !ok {
		return 0, errors.WithMessagef(ErrExecuteError, "failed to update (%s)", plan)
	}

	rows := int64(0)

	if err = scan.ForEach(us, func() (stop bool, err error) {
		rid := us.RID()
		for fieldName, ii := range indexes {
			val, werr := us.GetVal(fieldName)
			if werr != nil {
				return true, errors.WithMessage(ErrExecuteError, err.Error())
			}

			idx, werr := ii.Open()
			if werr != nil {
				return true, errors.WithMessage(ErrExecuteError, err.Error())
			}

			defer idx.Close()

			if werr = idx.Delete(val, rid); werr != nil {
				return true, errors.WithMessage(ErrExecuteError, err.Error())
			}
		}

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

func (p *IndexCommandsPlanner) ExecuteUpdate(stmt parse.UpdateStatement, trx scan.TRXInt) (int64, error) {
	var (
		plan Plan
		err  error
	)

	plan, err = planner.NewTablePlan(trx, stmt.TableName(), p.mdm)
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	plan, err = planner.NewSelectPlan(plan, stmt.Pred())
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	sc, err := plan.Open()
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	defer sc.Close()

	indexes, err := p.mdm.TableIndexes(stmt.TableName(), trx)
	if err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	us, ok := sc.(scan.UpdateScan)
	if !ok {
		return 0, errors.WithMessagef(ErrExecuteError, "failed to update (%s)", plan)
	}

	rows := int64(0)

	if err = scan.ForEach(us, func() (stop bool, err error) {
		for _, expr := range stmt.UpdateExpressions() {
			oldVal, werr := us.GetVal(expr.FieldName)
			if werr != nil {
				return true, werr
			}

			if werr = us.SetVal(expr.FieldName, expr.Value); werr != nil {
				return true, werr
			}

			rid := us.RID()

			ii, ok := indexes[expr.FieldName]
			if ok {
				idx, err2 := ii.Open()
				if err2 != nil {
					return true, err2
				}

				defer idx.Close()

				err2 = idx.Delete(oldVal, rid)
				if err2 != nil {
					return true, err2
				}

				err2 = idx.Insert(oldVal, rid)
				if err2 != nil {
					return true, err2
				}
			}
		}

		rows++

		return false, nil
	}); err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	return rows, nil
}

func (p *IndexCommandsPlanner) ExecuteCreateTable(stmt parse.CreateTableStatement, trx scan.TRXInt) (int64, error) {
	if err := p.mdm.CreateTable(stmt.TableName(), stmt.Schema(), trx); err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	return 0, nil
}

func (p *IndexCommandsPlanner) ExecuteCreateIndex(stmt parse.CreateIndexStatement, trx scan.TRXInt) (int64, error) {
	if len(stmt.Fields()) > 1 {
		return 0, errors.WithMessage(ErrExecuteError, "composite keys isn't suported")
	}

	if err := p.mdm.CreateIndex(stmt.IndexName(), stmt.TableName(), stmt.IndexType(), stmt.Fields()[0], trx); err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	return 0, nil
}

func (p *IndexCommandsPlanner) ExecuteCreateView(stmt parse.CreateViewStatement, trx scan.TRXInt) (int64, error) {
	if err := p.mdm.CreateView(stmt.ViewName(), stmt.ViewDef(), trx); err != nil {
		return 0, errors.WithMessage(ErrExecuteError, err.Error())
	}

	return 0, nil
}
