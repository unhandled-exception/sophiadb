package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
)

type SQLCreateViewStatementTestSuite struct {
	suite.Suite
}

func TestSQLCreateViewStatementTestSuite(t *testing.T) {
	suite.Run(t, new(SQLCreateViewStatementTestSuite))
}

var _ parse.CreateViewStatement = &parse.SQLCreateViewStatement{}

func (ts *SQLCreateViewStatementTestSuite) TestStatement_Ok() {
	t := ts.T()

	tt := []struct {
		query  string
		parsed string
	}{
		{
			query:  "create view view1 as select field1, field2 from table1, table2",
			parsed: "create view view1 as select field1, field2 from table1, table2",
		},
		{
			query:  "create view view1 as select field1, field2 from table1, table2 where 1=1 and field1=field2 and field1=125 and field2=12345 and field3='value'",
			parsed: "create view view1 as select field1, field2 from table1, table2 where 1 = 1 and field1 = field2 and field1 = 125 and field2 = 12345 and field3 = 'value'",
		},
	}

	for _, tc := range tt {
		sut, err := parse.NewSQLCreateViewStatement(tc.query)
		assert.NoErrorf(t, err, "error: %s for: %s", err, tc.query)

		if err == nil {
			assert.Equal(t, tc.parsed, sut.String())
		}
	}
}

func (ts *SQLCreateViewStatementTestSuite) TestStatement_Fail() {
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
			query: "create table table1 (id int64)",
			err:   parse.ErrInvalidStatement,
		},
		{
			query: "create view view1 ",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create view view ",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create view view1 as ",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create view view1 as insert",
			err:   parse.ErrBadSyntax,
		},
	}

	for _, tc := range tt {
		_, err := parse.NewSQLCreateViewStatement(tc.query)

		assert.ErrorIsf(t, err, tc.err, "no error for: %s", tc.query)
	}
}
