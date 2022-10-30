package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/parse"
)

type SQLCreateTableStatementTestSuite struct {
	suite.Suite
}

func TestSQLCreateTableStatementTestSuite(t *testing.T) {
	suite.Run(t, new(SQLCreateTableStatementTestSuite))
}

var _ parse.CreateTableStatement = &parse.SQLCreateTableStatement{}

func (ts *SQLCreateTableStatementTestSuite) TestStatement_Ok() {
	t := ts.T()

	tt := []struct {
		query  string
		parsed string
	}{
		{
			query:  "create table table1 (id int64, name varchar ( 100 ), age int8)",
			parsed: "create table table1 (id int64, name varchar(100), age int8)",
		},
		{
			query:  "create table table1 (id int64)",
			parsed: "create table table1 (id int64)",
		},
	}

	for _, tc := range tt {
		sut, err := parse.NewSQLCreateTableStatement(tc.query)
		assert.NoErrorf(t, err, "error: %s for: %s", err, tc.query)

		if err == nil {
			assert.Equal(t, tc.parsed, sut.String())
		}
	}
}

func (ts *SQLCreateTableStatementTestSuite) TestStatement_Fail() {
	t := ts.T()

	tt := []struct {
		query string
		err   error
	}{
		{
			query: "select field from table1",
			err:   parse.ErrInvalidStatement,
		},
		{
			query: "refresh all tables",
			err:   parse.ErrInvalidStatement,
		},
		{
			query: "create index table1 (id int64)",
			err:   parse.ErrInvalidStatement,
		},
		{
			query: "create table1",
			err:   parse.ErrInvalidStatement,
		},
		{
			query: "create table",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 ",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 ()",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 id int64, name varchar(100), age int8)",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 (id int64, name varchar(100), age int8",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 (name varchar",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 (name varchar(100",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 (name varchar(ddd))",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 (id, name varchar(100), age int8)",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 (int64, name varchar(100), age int8)",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 (id int64, name, age int8)",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 (id int64,)",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create table table1 (,,,)",
			err:   parse.ErrBadSyntax,
		},
	}

	for _, tc := range tt {
		_, err := parse.NewSQLCreateTableStatement(tc.query)

		assert.ErrorIsf(t, err, tc.err, "no error for: %s", tc.query)
	}
}
