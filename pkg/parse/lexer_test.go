package parse_test

import (
	"fmt"
	"testing"

	"github.com/bzick/tokenizer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
)

const (
	tokenDelim = iota + 1
	tokenSQLKeyword
)

type SQLLexerTestSuite struct {
	suite.Suite
}

var _ parse.Lexer = parse.SQLLexer{}

func TestSQLLexerTestSuite(t *testing.T) {
	suite.Run(t, new(SQLLexerTestSuite))
}

func (ts *SQLLexerTestSuite) TestMatchKeyword() {
	t := ts.T()

	s := "select field from table"

	sut := parse.NewLexer(s)
	defer sut.Close()

	require.True(t, sut.MatchKeyword("select"))
	require.False(t, sut.MatchKeyword("field"))
}

func (ts *SQLLexerTestSuite) TestTokenizerLib() {
	t := ts.T()

	lex := tokenizer.New()
	lex.AllowKeywordUnderscore()
	lex.AllowNumbersInKeyword()
	lex.DefineStringToken(tokenizer.TokenString, "'", "'").SetEscapeSymbol(tokenizer.BackSlash)
	lex.DefineTokens(tokenDelim, []string{",", "=", "."})
	lex.DefineTokens(tokenSQLKeyword, []string{"select", "from", "where", "and", "or"})

	stream := lex.ParseString("select one, two_three, four.five, * from table1, table_2 where id = 1 and name = 'john glen' or title=name")
	defer stream.Close()

	result := make([]string, 0)

	for stream.IsValid() {
		tok := stream.CurrentToken()

		result = append(
			result,
			fmt.Sprintf("%s: %s", tokenKeyString(tok.Key()), tok.Value()),
		)

		stream.GoNext()
	}

	assert.Equal(t, []string{
		"SQLKeyword: select",
		"TokenKeyword: one",
		"Delim: ,",
		"TokenKeyword: two_three",
		"Delim: ,",
		"TokenKeyword: four",
		"Delim: .",
		"TokenKeyword: five",
		"Delim: ,",
		"TokenUnknown: *",
		"SQLKeyword: from",
		"TokenKeyword: table1",
		"Delim: ,",
		"TokenKeyword: table_2",
		"SQLKeyword: where",
		"TokenKeyword: id",
		"Delim: =",
		"TokenInteger: 1",
		"SQLKeyword: and",
		"TokenKeyword: name",
		"Delim: =",
		"TokenString: 'john glen'",
		"SQLKeyword: or",
		"TokenKeyword: title",
		"Delim: =",
		"TokenKeyword: name",
	},
		result,
	)
}

func tokenKeyString(key tokenizer.TokenKey) string {
	switch key {
	case tokenizer.TokenUnknown:
		return "TokenUnknown"
	case tokenizer.TokenStringFragment:
		return "TokenStringFragment"
	case tokenizer.TokenString:
		return "TokenString"
	case tokenizer.TokenFloat:
		return "TokenFloat"
	case tokenizer.TokenInteger:
		return "TokenInteger"
	case tokenizer.TokenKeyword:
		return "TokenKeyword"
	case tokenizer.TokenUndef:
		return "TokenUndef"
	case tokenDelim:
		return "Delim"
	case tokenSQLKeyword:
		return "SQLKeyword"
	}

	return fmt.Sprintf("[unknown token %d]", key)
}
