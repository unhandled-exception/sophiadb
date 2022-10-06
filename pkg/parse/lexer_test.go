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

	sut := parse.NewLexer("select field from table")
	defer sut.Close()

	require.True(t, sut.MatchKeyword("select"))
	require.False(t, sut.MatchKeyword("field"))
}

func (ts *SQLLexerTestSuite) TestEatKeyword() {
	t := ts.T()

	sut := parse.NewLexer("select field from table")
	defer sut.Close()

	require.NoError(t, sut.EatKeyword("select"))
	require.ErrorIs(t, sut.EatKeyword("field"), parse.ErrBadSyntax)
}

func (ts *SQLLexerTestSuite) TestMatchDelim() {
	t := ts.T()

	sut := parse.NewLexer(" , var")
	defer sut.Close()

	require.True(t, sut.MatchDelim(","))
	require.False(t, sut.MatchDelim("."))
}

func (ts *SQLLexerTestSuite) TestEatDelim() {
	t := ts.T()

	sut := parse.NewLexer(" , var")
	defer sut.Close()

	require.NoError(t, sut.EatDelim(","))
	require.ErrorIs(t, sut.EatDelim("."), parse.ErrBadSyntax)
}

func (ts *SQLLexerTestSuite) TestMatchID() {
	t := ts.T()

	sut1 := parse.NewLexer(" name = 'title'")
	defer sut1.Close()

	require.True(t, sut1.MatchID())

	sut2 := parse.NewLexer(" select name")
	defer sut2.Close()

	require.False(t, sut2.MatchID())
}

func (ts *SQLLexerTestSuite) TestEatID() {
	t := ts.T()

	sut := parse.NewLexer("name = var")
	defer sut.Close()

	id, err := sut.EatID()
	require.NoError(t, err)
	assert.Equal(t, "name", id)

	_, err = sut.EatID()
	require.ErrorIs(t, err, parse.ErrBadSyntax)
}

func (ts *SQLLexerTestSuite) TestMatchStringConstant() {
	t := ts.T()

	sut1 := parse.NewLexer(" 'title name'")
	defer sut1.Close()

	require.True(t, sut1.MatchStringConstant())

	sut2 := parse.NewLexer("some text")
	defer sut2.Close()

	require.False(t, sut2.MatchStringConstant())
}

func (ts *SQLLexerTestSuite) TestEatStringConstant() {
	t := ts.T()

	sut := parse.NewLexer(" 'title name' some text")
	defer sut.Close()

	id, err := sut.EatStringConstant()
	require.NoError(t, err)
	assert.Equal(t, "title name", id)

	_, err = sut.EatStringConstant()
	require.ErrorIs(t, err, parse.ErrBadSyntax)
}

func (ts *SQLLexerTestSuite) TestMatchIntConstant() {
	t := ts.T()

	sut1 := parse.NewLexer(" 242424454353 ")
	defer sut1.Close()

	require.True(t, sut1.MatchIntConstant())

	sut2 := parse.NewLexer("some text")
	defer sut2.Close()

	require.False(t, sut2.MatchIntConstant())
}

func (ts *SQLLexerTestSuite) TestEatIntConstant() {
	t := ts.T()

	sut1 := parse.NewLexer(" 123,457")
	defer sut1.Close()

	val, err := sut1.EatIntConstant()
	require.NoError(t, err)
	assert.EqualValues(t, 123, val)

	_, err = sut1.EatIntConstant()
	require.ErrorIs(t, err, parse.ErrBadSyntax)

	sut2 := parse.NewLexer(" 123.457")

	_, err = sut2.EatIntConstant()
	require.ErrorIs(t, err, parse.ErrBadSyntax)
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
