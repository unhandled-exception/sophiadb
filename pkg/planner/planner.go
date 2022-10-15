package planner

import (
	"github.com/unhandled-exception/sophiadb/pkg/parse"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type QueryPlanner interface {
	CreatePlan(stmt parse.SelectStatement, trx scan.TRXInt) (plan Plan, err error)
}

type CommandsPlanner interface {
	ExecuteInsert(stmt parse.InsertStatement, trx scan.TRXInt) (rows int, err error)
	ExecuteDelete(stmt parse.DeleteStatement, trx scan.TRXInt) (rows int, err error)
	ExecuteUpdate(stmt parse.UpdateStatement, trx scan.TRXInt) (rows int, err error)
	ExecuteCreateTable(stmt parse.CreateTableStatement, trx scan.TRXInt) (rows int, err error)
	ExecuteCreateIndex(stmt parse.CreateIndexStatement, trx scan.TRXInt) (rows int, err error)
	ExecuteCreateView(stmt parse.CreateViewStatement, trx scan.TRXInt) (rows int, err error)
}

type Planner interface {
	CreateQueryPlan(query string, trx scan.TRXInt) (plan Plan, err error)
	ExecuteCommand(cmd string, trx scan.TRXInt) (rows int, err error)
}

type SQLPlanner struct {
	queryPlanner    QueryPlanner
	commandsPlanner CommandsPlanner
}

func NewSQLPlanner(queryPlanner QueryPlanner, commandsPlanner CommandsPlanner) *SQLPlanner {
	p := &SQLPlanner{
		queryPlanner:    queryPlanner,
		commandsPlanner: commandsPlanner,
	}

	return p
}

func (p *SQLPlanner) CreateQueryPlan(query string, trx scan.TRXInt) (Plan, error) {
	stmtType, stmt, err := parse.ParseQuery(query)

	switch {
	case stmtType != parse.StmtSelect:
		return nil, parse.ErrBadSyntax
	case err != nil:
		return nil, err
	}

	plan, err := p.queryPlanner.CreatePlan(stmt.(*parse.SQLSelectStatement), trx)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func (p *SQLPlanner) ExecuteCommand(cmd string, trx scan.TRXInt) (int, error) {
	stmtType, stmt, err := parse.ParseQuery(cmd)
	if err != nil {
		return 0, err
	}

	//nolint:forcetypeassert,exhaustive
	switch stmtType {
	case parse.StmtInsert:
		return p.commandsPlanner.ExecuteInsert(stmt.(parse.InsertStatement), trx)
	case parse.StmtDelete:
		return p.commandsPlanner.ExecuteDelete(stmt.(parse.DeleteStatement), trx)
	case parse.StmtUpdate:
		return p.commandsPlanner.ExecuteUpdate(stmt.(parse.UpdateStatement), trx)
	case parse.StmtCreateTable:
		return p.commandsPlanner.ExecuteCreateTable(stmt.(parse.CreateTableStatement), trx)
	case parse.StmtCreateIndex:
		return p.commandsPlanner.ExecuteCreateIndex(stmt.(parse.CreateIndexStatement), trx)
	case parse.StmtCreateView:
		return p.commandsPlanner.ExecuteCreateView(stmt.(parse.CreateViewStatement), trx)
	}

	return 0, parse.ErrInvalidStatement
}
