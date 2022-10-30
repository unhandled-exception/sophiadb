package parse

import "github.com/pkg/errors"

type StmtType int

const (
	StmtUnknown StmtType = iota
	StmtSelect
	StmtInsert
	StmtDelete
	StmtUpdate
	StmtCreateTable
	StmtCreateIndex
	StmtCreateView
)

type (
	newStatementFunc func(string) (Statement, error)
)

var statementsConstructors = []struct {
	stmtType StmtType
	creator  newStatementFunc
}{
	{stmtType: StmtSelect, creator: func(q string) (Statement, error) { return NewSQLSelectStatement(q) }},
	{stmtType: StmtInsert, creator: func(q string) (Statement, error) { return NewSQLInsertStatement(q) }},
	{stmtType: StmtDelete, creator: func(q string) (Statement, error) { return NewSQLDeleteStatement(q) }},
	{stmtType: StmtUpdate, creator: func(q string) (Statement, error) { return NewSQLUpdateStatement(q) }},
	{stmtType: StmtCreateTable, creator: func(q string) (Statement, error) { return NewSQLCreateTableStatement(q) }},
	{stmtType: StmtCreateIndex, creator: func(q string) (Statement, error) { return NewSQLCreateIndexStatement(q) }},
	{stmtType: StmtCreateView, creator: func(q string) (Statement, error) { return NewSQLCreateViewStatement(q) }},
}

func ParseQuery(q string) (StmtType, Statement, error) {
	for _, c := range statementsConstructors {
		stmt, err := c.creator(q)

		if errors.Is(err, ErrInvalidStatement) {
			continue
		}

		if err != nil {
			return c.stmtType, nil, err
		}

		return c.stmtType, stmt, nil
	}

	return StmtUnknown, nil, ErrInvalidStatement
}
