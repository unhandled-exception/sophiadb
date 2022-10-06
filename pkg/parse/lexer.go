package parse

import "github.com/bzick/tokenizer"

type Lexer interface {
	MatchDelim(delim string) bool
	MatchIntConstant() bool
	MatchStringConstant() bool
	MatchKeyword(keyword string) bool
	MatchID() bool

	EatDelim(delim string) error
	EatIntConstant() (int64, error)
	EatStringConstant() (string, error)
	EatKeyword(keyword string) error
	EatID() (string, error)
}

const (
	tokenDelim = iota + 1
	tokenSQLKeyword
)

var sqlKeywords []string = []string{
	"select", "from", "where", "and",
	"insert", "into", "values",
	"delete",
	"update", "set",
	"create", "table",
	"varchar", "int", "int64", "int8", "view", "as",
	"index", "on",
}

type SQLLexer struct {
	lexStream *tokenizer.Stream
}

func NewLexer(text string) SQLLexer {
	lex := tokenizer.New()
	lex.AllowKeywordUnderscore()
	lex.AllowNumbersInKeyword()
	lex.DefineStringToken(tokenizer.TokenString, "'", "'").SetEscapeSymbol(tokenizer.BackSlash)
	lex.DefineTokens(tokenDelim, []string{",", "=", "."})
	lex.DefineTokens(tokenSQLKeyword, sqlKeywords)

	l := SQLLexer{
		lexStream: lex.ParseString(text),
	}

	return l
}

func (l SQLLexer) Close() {
	l.lexStream.Close()
}

//nolint:unused
func (l SQLLexer) nextToken() bool {
	if !l.lexStream.IsValid() {
		return false
	}

	l.lexStream.GoNext()

	return true
}

func (l SQLLexer) MatchDelim(delim string) bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) MatchIntConstant() bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) MatchStringConstant() bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) MatchKeyword(keyword string) bool {
	tok := l.lexStream.CurrentToken()

	return tok.Key() == tokenSQLKeyword && tok.ValueString() == keyword
}

func (l SQLLexer) MatchID() bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) EatDelim(delim string) error {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) EatIntConstant() (int64, error) {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) EatStringConstant() (string, error) {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) EatKeyword(keyword string) error {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) EatID() (string, error) {
	panic("not implemented") // TODO: Implement
}
