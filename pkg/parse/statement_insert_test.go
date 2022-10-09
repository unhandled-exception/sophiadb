package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
)

type SQLInsertStatementTestSuite struct {
	suite.Suite
}

func TestSQLInsertStatementTestSuite(t *testing.T) {
	suite.Run(t, new(SQLInsertStatementTestSuite))
}

var _ parse.InsertStatement = &parse.SQLInsertStatement{}

func (ts *SQLInsertStatementTestSuite) TestStatement_Ok() {
	t := ts.T()

	tt := []struct {
		query  string
		parsed string
	}{
		{
			query:  "insert into table1 (field1, field_2, field3) values (124, 12345, 'test')",
			parsed: "insert into table1 (field1, field_2, field3) values (124, 12345, 'test')",
		},
	}

	sut := &parse.SQLInsertStatement{}

	for _, tc := range tt {
		err := sut.Parse(tc.query)
		require.NoErrorf(t, err, "error: %s for: %s", err, tc.query)

		assert.Equal(t, tc.parsed, sut.String())
	}
}

func (ts *SQLInsertStatementTestSuite) TestStatement_Fail() {
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
			query: "table1 (field1, field_2, field3) values (124, 12345, 'test') tail",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert into table1 (field1, field_2, field3) values (124, 12345, 'test') tail",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert table1 (field1, field_2, field3) values (124, 12345, 'test')",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert into  (field1, field_2, field3) values (124, 12345, 'test')",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert into table1 field1, field_2, field3) values (124, 12345, 'test')",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert into table1 (field1, field_2, field3 values (124, 12345, 'test')",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert into table1 (field1, field_2, field3) (124, 12345, 'test')",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert into table1 (field1, field_2, field3) values 124, 12345, 'test')",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert into table1 (field1, field_2, field3) values (124, 12345, 'test'",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert into table1 (field1, -111, field3) values (124, 12345, 'test')",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert into table1 (field1, -111, field3) values (124, 12345+1, 'test')",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "insert into table1 (field1, field_2, field3) values (124, ssss, 'test')",
			err:   parse.ErrBadSyntax,
		},
	}

	sut := &parse.SQLInsertStatement{}

	for _, tc := range tt {
		err := sut.Parse(tc.query)
		require.ErrorIsf(t, err, tc.err, "no error for: %s", tc.query)
	}
}
