package parse_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
)

type SQLLexerTestSuite struct {
	suite.Suite
}

var _ parse.Lexer = parse.SQLLexer{}

func TestSQLLexerTestSuite(t *testing.T) {
	suite.Run(t, new(SQLLexerTestSuite))
}
