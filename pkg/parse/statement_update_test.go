package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
)

type SQLUpdateStatementTestSuite struct {
	suite.Suite
}

func TestSQLUpdateStatementTestSuite(t *testing.T) {
	suite.Run(t, new(SQLUpdateStatementTestSuite))
}

var _ parse.UpdateStatement = &parse.SQLUpdateStatement{}

func (ts *SQLUpdateStatementTestSuite) TestStatement_Ok() {
	t := ts.T()

	tt := []struct {
		query  string
		parsed string
	}{
		{
			query:  "update table1 set field1=123, field2=12345, field3='value'",
			parsed: "update table1 set field1 = 123, field2 = 12345, field3 = 'value'",
		},
		{
			query:  "update table1 set field1 = 123, field2 = 12345, field3 = 'value' where 1=1 and field1=field2 and field1=125 and field2=12345 and field3='value'",
			parsed: "update table1 set field1 = 123, field2 = 12345, field3 = 'value' where 1 = 1 and field1 = field2 and field1 = 125 and field2 = 12345 and field3 = 'value'",
		},
	}

	for _, tc := range tt {
		sut, err := parse.NewSQLUpdateStatement(tc.query)
		assert.NoErrorf(t, err, "error: %s for: %s", err, tc.query)

		if err == nil {
			assert.Equal(t, tc.parsed, sut.String())
		}
	}
}

func (ts *SQLUpdateStatementTestSuite) TestStatement_Fail() {
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
			query: "update table1 set field1=123, field2=12345, field3='value' tail",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "update",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "update table1 ",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "update table1 set ",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "update table1 set field1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "update table1 set field1=dddd",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "update table1 set field1=123, field2",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "update table1 set field1 = 123, field2 = 12345, field3 = 'value' where ",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "update table1 set field1 = 123, field2 = 12345, field3 = 'value' where 1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "update table1 set field1 = 123, field2 = 12345, field3 = 'value' where 1=1 and field1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "update table1 set field1 = 123, field2 = 12345, field3 = 'value' where 1=1 or field1=field2 ",
			err:   parse.ErrBadSyntax,
		},
	}

	for _, tc := range tt {
		_, err := parse.NewSQLUpdateStatement(tc.query)

		assert.ErrorIsf(t, err, tc.err, "no error for: %s", tc.query)
	}
}
