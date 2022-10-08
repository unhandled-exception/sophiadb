package parse

type StmtType int

const (
	StmtUnknown StmtType = iota
	StmtQuery
	StmtInsert
	StmtDelete
	StmtUpdate
	StmtCreateTable
	StmtCreateView
)

type Parser interface {
	Parse(string) (StmtType, interface{}, error)
}

type SQLParser struct {
	statements map[StmtType]Statement
}

func NewSQLParser() SQLParser {
	p := SQLParser{
		statements: map[StmtType]Statement{
			StmtQuery: &SQLSelectStatement{},
			// StmtInsert:      InsertStatement{},
			// StmtDelete:      DeleteStatement{},
			// StmtUpdate:      UpdateStatement{},
			// StmtCreateTable: CreateTableStatement{},
			// StmtCreateView:  CreateViewStatement{},
		},
	}

	return p
}

func Parse(string) (StmtType, interface{}, error) {
	return StmtUnknown, nil, ErrInvalidStatement
}
