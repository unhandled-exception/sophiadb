package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
)

type ParseQueryTestSuite struct {
	suite.Suite
}

func TestParseQueryTestSuite(t *testing.T) {
	suite.Run(t, new(ParseQueryTestSuite))
}

func (ts *ParseQueryTestSuite) TestParseQuery_Ok() {
	t := ts.T()

	tt := []struct {
		query    string
		stmtType parse.StmtType
		err      error
	}{
		{
			query:    "select field1 from table1",
			stmtType: parse.StmtSelect,
		},
		{
			query:    "insert into table1 (f1) values (1)",
			stmtType: parse.StmtInsert,
		},
		{
			query:    "delete from table1",
			stmtType: parse.StmtDelete,
		},
		{
			query:    "update table1 set f1 = 1",
			stmtType: parse.StmtUpdate,
		},
		{
			query:    "create table table1 (f int64)",
			stmtType: parse.StmtCreateTable,
		},
		{
			query:    "create index index1 on table1 (f1)",
			stmtType: parse.StmtCreateIndex,
		},
		{
			query:    "create view view1 as select f1 from table1",
			stmtType: parse.StmtCreateView,
		},
	}

	for _, tc := range tt {
		st, stmt, err := parse.ParseQuery(tc.query)

		assert.Equal(t, tc.stmtType, st)
		assert.NoError(t, err)

		if err != nil {
			assert.Equal(t, tc.query, stmt.String())
		}
	}
}

func (ts *ParseQueryTestSuite) TestParseQuery_Fail() {
	t := ts.T()

	tt := []struct {
		query    string
		stmtType parse.StmtType
		err      error
	}{
		{
			query:    "refresh all tables",
			stmtType: parse.StmtUnknown,
			err:      parse.ErrInvalidStatement,
		},
		{
			query:    "select field1 ",
			stmtType: parse.StmtSelect,
			err:      parse.ErrBadSyntax,
		},
		{
			query:    "insert into table1 ",
			stmtType: parse.StmtInsert,
			err:      parse.ErrBadSyntax,
		},
		{
			query:    "delete from",
			stmtType: parse.StmtDelete,
			err:      parse.ErrBadSyntax,
		},
		{
			query:    "update table1",
			stmtType: parse.StmtUpdate,
			err:      parse.ErrBadSyntax,
		},
		{
			query:    "create table table1",
			stmtType: parse.StmtCreateTable,
			err:      parse.ErrBadSyntax,
		},
		{
			query:    "create index index1",
			stmtType: parse.StmtCreateIndex,
			err:      parse.ErrBadSyntax,
		},
		{
			query:    "create view view1",
			stmtType: parse.StmtCreateView,
			err:      parse.ErrBadSyntax,
		},
	}

	for _, tc := range tt {
		st, stmt, err := parse.ParseQuery(tc.query)

		assert.Equal(t, tc.stmtType, st)
		assert.ErrorIs(t, err, tc.err)
		assert.Nil(t, stmt)
	}
}
