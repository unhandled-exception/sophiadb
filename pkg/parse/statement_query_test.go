package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
)

type SQLSelectStatementTestSuite struct {
	suite.Suite
}

func TestSQLSelectStatementTestSuite(t *testing.T) {
	suite.Run(t, new(SQLSelectStatementTestSuite))
}

var _ parse.SelectStatement = &parse.SQLSelectStatement{}

func (ts *SQLSelectStatementTestSuite) TestSelectQuery_Ok() {
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

	sut := &parse.SQLSelectStatement{}

	for _, tc := range tt {
		err := sut.Parse(tc.query)
		require.NoErrorf(t, err, "error: %s for: %s", err, tc.query)

		assert.Equal(t, tc.parsed, sut.String())
	}
}

func (ts *SQLSelectStatementTestSuite) TestSelectQuery_Fail() {
	t := ts.T()

	tt := []struct {
		query string
		err   error
	}{
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
	}

	sut := &parse.SQLSelectStatement{}

	for _, tc := range tt {
		err := sut.Parse(tc.query)
		require.ErrorIsf(t, err, tc.err, "no error for: %s", tc.query)
	}
}
