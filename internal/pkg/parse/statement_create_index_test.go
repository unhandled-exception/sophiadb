package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/parse"
)

type SQLCreateIndexStatementTestSuite struct {
	suite.Suite
}

func TestSQLCreateIndexStatementTestSuite(t *testing.T) {
	suite.Run(t, new(SQLCreateIndexStatementTestSuite))
}

var _ parse.CreateIndexStatement = &parse.SQLCreateIndexStatement{}

func (ts *SQLCreateIndexStatementTestSuite) TestStatement_Ok() {
	t := ts.T()

	tt := []struct {
		query     string
		parsed    string
		indexType indexes.IndexType
	}{
		{
			query:     "create index index1 on table1 (field1)",
			parsed:    "create index index1 on table1 (field1) using hash",
			indexType: indexes.HashIndexType,
		},
		{
			query:     "create index index1 on table1 (field1, field2, field3)",
			parsed:    "create index index1 on table1 (field1, field2, field3) using hash",
			indexType: indexes.HashIndexType,
		},
		{
			query:     "create index index1 on table1 (field1) using hash",
			parsed:    "create index index1 on table1 (field1) using hash",
			indexType: indexes.HashIndexType,
		},
		{
			query:     "create index index1 on table1 (field1) using btree",
			parsed:    "create index index1 on table1 (field1) using btree",
			indexType: indexes.BTreeIndexType,
		},
	}

	for _, tc := range tt {
		sut, err := parse.NewSQLCreateIndexStatement(tc.query)
		assert.NoErrorf(t, err, "error: %s for: %s", err, tc.query)

		if err == nil {
			assert.Equal(t, tc.parsed, sut.String())
			assert.EqualValues(t, tc.indexType, sut.IndexType())
		}
	}
}

func (ts *SQLCreateIndexStatementTestSuite) TestStatement_Fail() {
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
			query: "create",
			err:   parse.ErrInvalidStatement,
		},
		{
			query: "create index",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create index index1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create index index1 on",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create index index1 on table1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create index index1 on table1 (",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create index index1 on table1 ()",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create index index1 on table1 (field1",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create index index1 on table1 (field1) tail",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create index index1 on table1 (field1) using ",
			err:   parse.ErrBadSyntax,
		},
		{
			query: "create index index1 on table1 (field1) using gist",
			err:   parse.ErrBadSyntax,
		},
	}

	for _, tc := range tt {
		_, err := parse.NewSQLCreateIndexStatement(tc.query)

		assert.ErrorIsf(t, err, tc.err, "no error for: %s", tc.query)
	}
}
