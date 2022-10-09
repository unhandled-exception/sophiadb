package parse

type StmtType int

const (
	StmtUnknown StmtType = iota
	StmtQuery
	StmtInsert
	StmtDelete
	StmtUpdate
	StmtCreateTable
	StmtCreateIndex
	StmtCreateView
)

type Parser interface {
	Parse(string) (StmtType, interface{}, error)
}

type (
	newStatementFunc func(string) (Statement, error)
	SQLParser        struct {
		statements map[StmtType]newStatementFunc
	}
)

func NewSQLParser() SQLParser {
	p := SQLParser{
		statements: map[StmtType]newStatementFunc{
			StmtQuery:       func(q string) (Statement, error) { return NewSQLSelectStatement(q) },
			StmtInsert:      func(q string) (Statement, error) { return NewSQLInsertStatement(q) },
			StmtDelete:      func(q string) (Statement, error) { return NewSQLDeleteStatement(q) },
			StmtUpdate:      func(q string) (Statement, error) { return NewSQLUpdateStatement(q) },
			StmtCreateTable: func(q string) (Statement, error) { return NewSQLCreateTableStatement(q) },
			StmtCreateIndex: func(q string) (Statement, error) { return NewSQLCreateIndexStatement(q) },
			StmtCreateView:  func(q string) (Statement, error) { return NewSQLCreateViewStatement(q) },
		},
	}

	return p
}

func Parse(string) (StmtType, interface{}, error) {
	return StmtUnknown, nil, ErrInvalidStatement
}
