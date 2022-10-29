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

	sut1 := parse.NewSQLLexer("SELECT field FROM table")
	defer sut1.Close()

	ok, err := sut1.MatchKeyword("select")
	require.True(t, ok)
	require.NoError(t, err)

	ok, err = sut1.MatchKeyword("insert")
	require.False(t, ok)
	require.ErrorIs(t, err, parse.ErrUnmatchedKeyword)

	sut2 := parse.NewSQLLexer("one from table")
	defer sut2.Close()

	ok, err = sut2.MatchKeyword("select")
	require.False(t, ok)
	require.ErrorIs(t, err, parse.ErrBadSyntax)

	sut3 := parse.NewSQLLexer("")
	defer sut3.Close()

	ok, err = sut3.MatchKeyword("select")
	require.False(t, ok)
	require.ErrorIs(t, err, parse.ErrEOF)
}

func (ts *SQLLexerTestSuite) TestEatKeyword() {
	t := ts.T()

	sut := parse.NewSQLLexer("select field from table")
	defer sut.Close()

	require.NoError(t, sut.EatKeyword("select"))
	require.ErrorIs(t, sut.EatKeyword("field"), parse.ErrBadSyntax)
}

func (ts *SQLLexerTestSuite) TestMatchDelim() {
	t := ts.T()

	sut1 := parse.NewSQLLexer(" , var")
	defer sut1.Close()

	ok, err := sut1.MatchDelim(",")
	require.True(t, ok)
	require.NoError(t, err)

	ok, err = sut1.MatchDelim("=")
	require.False(t, ok)
	require.ErrorIs(t, err, parse.ErrUnmatchedDelim)

	sut2 := parse.NewSQLLexer("from table")
	defer sut2.Close()

	ok, err = sut2.MatchDelim("select")
	require.False(t, ok)
	require.ErrorIs(t, err, parse.ErrBadSyntax)

	sut3 := parse.NewSQLLexer("")
	defer sut3.Close()

	ok, err = sut3.MatchDelim(",")
	require.False(t, ok)
	require.ErrorIs(t, err, parse.ErrEOF)
}

func (ts *SQLLexerTestSuite) TestEatDelim() {
	t := ts.T()

	sut := parse.NewSQLLexer(" , var")
	defer sut.Close()

	require.NoError(t, sut.EatDelim(","))
	require.ErrorIs(t, sut.EatDelim("."), parse.ErrBadSyntax)
}

func (ts *SQLLexerTestSuite) TestMatchID() {
	t := ts.T()

	sut1 := parse.NewSQLLexer(" name = 'title'")
	defer sut1.Close()

	require.True(t, sut1.MatchID())

	sut2 := parse.NewSQLLexer(" select name")
	defer sut2.Close()

	require.False(t, sut2.MatchID())

	sut3 := parse.NewSQLLexer(", ")
	defer sut3.Close()

	require.False(t, sut3.MatchID())

	sut4 := parse.NewSQLLexer("")
	defer sut4.Close()

	require.False(t, sut4.MatchID())
}

func (ts *SQLLexerTestSuite) TestEatID() {
	t := ts.T()

	sut := parse.NewSQLLexer("Name = var")
	defer sut.Close()

	id, err := sut.EatID()
	require.NoError(t, err)
	assert.Equal(t, "name", id)

	_, err = sut.EatID()
	require.ErrorIs(t, err, parse.ErrBadSyntax)
}

func (ts *SQLLexerTestSuite) TestMatchStringConstant() {
	t := ts.T()

	sut1 := parse.NewSQLLexer(" 'title name'")
	defer sut1.Close()

	require.True(t, sut1.MatchStringConstant())

	sut2 := parse.NewSQLLexer("some text")
	defer sut2.Close()

	require.False(t, sut2.MatchStringConstant())

	sut3 := parse.NewSQLLexer("")
	defer sut3.Close()

	require.False(t, sut3.MatchStringConstant())
}

func (ts *SQLLexerTestSuite) TestEatStringConstant() {
	t := ts.T()

	sut := parse.NewSQLLexer(" 'title name' some text")
	defer sut.Close()

	s, err := sut.EatStringConstant()
	require.NoError(t, err)
	assert.Equal(t, "title name", s)

	_, err = sut.EatStringConstant()
	require.ErrorIs(t, err, parse.ErrBadSyntax)
}

func (ts *SQLLexerTestSuite) TestEatEscapedStringConstant() {
	t := ts.T()

	sut := parse.NewSQLLexer(` 'title \'name\' \n \'\tv\\\'' some text`)
	defer sut.Close()

	s, err := sut.EatStringConstant()
	require.NoError(t, err)
	assert.Equal(t, "title 'name' \n '\tv\\'", s)

	_, err = sut.EatStringConstant()
	require.ErrorIs(t, err, parse.ErrBadSyntax)
}

func (ts *SQLLexerTestSuite) TestMatchIntConstant() {
	t := ts.T()

	sut1 := parse.NewSQLLexer(" 242424454353 ")
	defer sut1.Close()

	require.True(t, sut1.MatchIntConstant())

	sut2 := parse.NewSQLLexer("some text")
	defer sut2.Close()

	require.False(t, sut2.MatchIntConstant())

	sut3 := parse.NewSQLLexer("")
	defer sut3.Close()

	require.False(t, sut3.MatchIntConstant())
}

func (ts *SQLLexerTestSuite) TestEatIntConstant() {
	t := ts.T()

	sut1 := parse.NewSQLLexer(" 123,457")
	defer sut1.Close()

	val, err := sut1.EatIntConstant()
	require.NoError(t, err)
	assert.EqualValues(t, 123, val)

	_, err = sut1.EatIntConstant()
	require.ErrorIs(t, err, parse.ErrBadSyntax)

	sut2 := parse.NewSQLLexer(" 123.457")

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
