package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/parse"
)

type SQLSelectStatementTestSuite struct {
	suite.Suite
}

func TestSQLSelectStatementTestSuite(t *testing.T) {
	suite.Run(t, new(SQLSelectStatementTestSuite))
}

var _ parse.SelectStatement = &parse.SQLSelectStatement{}

func (ts *SQLSelectStatementTestSuite) TestStatement_Ok() {
	t := ts.T()

	tt := []struct {
		query  string
		parsed string
	}{
		{
			query:  "select one, two, three from table1, table2, table_3",
			parsed: "select one, two, three from table1, table2, table_3",
		},
		{
			query:  "select one, two, three from table1 where 1=1 and field1=field2 and field1=125 and field2=12345 and field3='value'",
			parsed: "select one, two, three from table1 where 1 = 1 and field1 = field2 and field1 = 125 and field2 = 12345 and field3 = 'value'",
		},
	}

	for _, tc := range tt {
		sut, err := parse.NewSQLSelectStatement(tc.query)
		assert.NoErrorf(t, err, "error: %s for: %s", err, tc.query)

		if err == nil {
			assert.Equal(t, tc.parsed, sut.String())
		}
	}
}

func (ts *SQLSelectStatementTestSuite) TestStatement_Fail() {
	t := ts.T()

	tt := []struct {
		query string
		err   error
	}{
		{
			query: "refresh all tables",
			err:   parse.ErrInvalidStatement,
		},
		{
			query: "insert into table1 (field1) values (1)",
			err:   parse.ErrInvalidStatement,
		},
		{
			query: "select from table1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "select one, from table1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "select one from table1,",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "select one from",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "select one",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "select one from table1 where",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "select one from table1 where 1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "select one from table1 where 1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "select one from table1 where 1=1 or 1=2",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "select one from table1 where 1=1 and 1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "select one from table1 where 1=1 tail",
			err:   parse.ErrBadSyntax,
		},
	}

	for _, tc := range tt {
		_, err := parse.NewSQLSelectStatement(tc.query)

		assert.ErrorIsf(t, err, tc.err, "no error for: %s", tc.query)
	}
}
