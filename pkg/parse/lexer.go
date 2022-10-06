package parse

import (
	"github.com/bzick/tokenizer"
	"github.com/pkg/errors"
)

type Lexer interface {
	MatchKeyword(keyword string) bool
	MatchDelim(delim string) bool
	MatchIntConstant() bool
	MatchStringConstant() bool
	MatchID() bool

	EatKeyword(keyword string) error
	EatDelim(delim string) error
	EatIntConstant() (int64, error)
	EatStringConstant() (string, error)
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

func (l SQLLexer) nextToken() {
	l.lexStream.GoNext()
}

func (l SQLLexer) MatchKeyword(keyword string) bool {
	tok := l.lexStream.CurrentToken()

	return tok.Key() == tokenSQLKeyword && tok.ValueString() == keyword
}

func (l SQLLexer) MatchDelim(delim string) bool {
	tok := l.lexStream.CurrentToken()

	return tok.Key() == tokenDelim && tok.ValueString() == delim
}

func (l SQLLexer) MatchIntConstant() bool {
	return l.lexStream.CurrentToken().Key() == tokenizer.TokenInteger
}

func (l SQLLexer) MatchStringConstant() bool {
	return l.lexStream.CurrentToken().Key() == tokenizer.TokenString
}

func (l SQLLexer) MatchID() bool {
	return l.lexStream.CurrentToken().Key() == tokenizer.TokenKeyword
}

func (l SQLLexer) wrapLexerError(err error) error {
	return errors.WithMessagef(err, `near "%s" at %d:%d`, l.lexStream.CurrentToken().ValueString(), l.lexStream.CurrentToken().Line(), l.lexStream.CurrentToken().Offset())
}

func (l SQLLexer) EatKeyword(keyword string) error {
	if !l.MatchKeyword(keyword) {
		return l.wrapLexerError(ErrBadSyntax)
	}

	l.nextToken()

	return nil
}

func (l SQLLexer) EatDelim(delim string) error {
	if !l.MatchDelim(delim) {
		return l.wrapLexerError(ErrBadSyntax)
	}

	l.nextToken()

	return nil
}

func (l SQLLexer) EatIntConstant() (int64, error) {
	if !l.MatchIntConstant() {
		return 0, l.wrapLexerError(ErrBadSyntax)
	}

	val := l.lexStream.CurrentToken().ValueInt()

	l.nextToken()

	return val, nil
}

func (l SQLLexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", l.wrapLexerError(ErrBadSyntax)
	}

	id := l.lexStream.CurrentToken().ValueUnescapedString()

	l.nextToken()

	return id, nil
}

func (l SQLLexer) EatID() (string, error) {
	if !l.MatchID() {
		return "", l.wrapLexerError(ErrBadSyntax)
	}

	id := l.lexStream.CurrentToken().ValueString()

	l.nextToken()

	return id, nil
}
