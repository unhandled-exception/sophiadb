package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
)

type SQLDeleteStatementTestSuite struct {
	suite.Suite
}

func TestSQLDeleteStatementTestSuite(t *testing.T) {
	suite.Run(t, new(SQLDeleteStatementTestSuite))
}

var _ parse.DeleteStatement = &parse.SQLDeleteStatement{}

func (ts *SQLDeleteStatementTestSuite) TestStatement_Ok() {
	t := ts.T()

	tt := []struct {
		query  string
		parsed string
	}{
		{
			query:  "delete from table1",
			parsed: "delete from table1",
		},
		{
			query:  "delete from table1 where 1=1 and field1=field2 and field1=125 and field2=12345 and field3='value'",
			parsed: "delete from table1 where 1 = 1 and field1 = field2 and field1 = 125 and field2 = 12345 and field3 = 'value'",
		},
	}

	for _, tc := range tt {
		sut, err := parse.NewSQLDeleteStatement(tc.query)
		assert.NoErrorf(t, err, "error: %s for: %s", err, tc.query)

		if err == nil {
			assert.Equal(t, tc.parsed, sut.String())
		}
	}
}

func (ts *SQLDeleteStatementTestSuite) TestStatement_Fail() {
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
			query: "delete ",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "delete from",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "delete from where",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "delete from table1 where",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "delete from table1 where 1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "delete from table1 where 1=1 tail",
			err:   parse.ErrBadSyntax,
		},
	}

	for _, tc := range tt {
		_, err := parse.NewSQLDeleteStatement(tc.query)

		assert.ErrorIsf(t, err, tc.err, "no error for: %s", tc.query)
	}
}
