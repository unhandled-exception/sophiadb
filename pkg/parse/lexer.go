package parse

import (
	"strings"

	"github.com/bzick/tokenizer"
	"github.com/pkg/errors"
)

type Lexer interface {
	MatchKeyword(keyword string) (bool, error)
	MatchDelim(delim string) (bool, error)
	MatchIntConstant() bool
	MatchStringConstant() bool
	MatchID() bool

	EatKeyword(keyword string) error
	EatDelim(delim string) error
	EatIntConstant() (int64, error)
	EatStringConstant() (string, error)
	EatID() (string, error)

	EOF() bool
	WrapLexerError(err error) error
}

const (
	tokenDelim = iota + 1
	tokenSQLKeyword
)

var sqlKeywords = map[string]struct{}{
	"select":  {},
	"from":    {},
	"where":   {},
	"and":     {},
	"insert":  {},
	"into":    {},
	"values":  {},
	"delete":  {},
	"update":  {},
	"set":     {},
	"create":  {},
	"table":   {},
	"varchar": {},
	"int":     {},
	"int64":   {},
	"int8":    {},
	"view":    {},
	"as":      {},
	"index":   {},
	"on":      {},
}

type SQLLexer struct {
	lexStream *tokenizer.Stream
}

func NewSQLLexer(text string) SQLLexer {
	lex := tokenizer.New()
	lex.AllowKeywordUnderscore()
	lex.AllowNumbersInKeyword()
	lex.DefineStringToken(tokenizer.TokenString, "'", "'").SetEscapeSymbol(tokenizer.BackSlash)
	lex.DefineTokens(tokenDelim, []string{",", "=", ".", "(", ")", "+", "-", "*", "/", "%"})
	lex.SetWhiteSpaces([]byte{' ', '\t', '\n', '\r'})

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

func (l SQLLexer) EOF() bool {
	return !l.lexStream.IsValid()
}

func (l SQLLexer) MatchKeyword(keyword string) (bool, error) {
	if !l.lexStream.IsValid() || l.lexStream.CurrentToken() == nil {
		return false, ErrEOF
	}

	tok := l.lexStream.CurrentToken()

	var err error

	v := strings.ToLower(tok.ValueString())

	if _, ok := sqlKeywords[v]; !ok {
		err = ErrBadSyntax
	} else if v != strings.ToLower(keyword) {
		err = ErrUnmatchedKeyword
	}

	return err == nil, err
}

func (l SQLLexer) MatchDelim(delim string) (bool, error) {
	if !l.lexStream.IsValid() || l.lexStream.CurrentToken() == nil {
		return false, ErrEOF
	}

	tok := l.lexStream.CurrentToken()

	var err error

	if tok.Key() != tokenDelim {
		err = ErrBadSyntax
	} else if tok.ValueString() != delim {
		err = ErrUnmatchedDelim
	}

	return err == nil, err
}

func (l SQLLexer) MatchIntConstant() bool {
	if !l.lexStream.IsValid() || l.lexStream.CurrentToken() == nil {
		return false
	}

	tok := l.lexStream.CurrentToken()

	return tok.Key() == tokenizer.TokenInteger
}

func (l SQLLexer) MatchStringConstant() bool {
	if !l.lexStream.IsValid() || l.lexStream.CurrentToken() == nil {
		return false
	}

	tok := l.lexStream.CurrentToken()

	return tok.Key() == tokenizer.TokenString
}

func (l SQLLexer) MatchID() bool {
	if !l.lexStream.IsValid() || l.lexStream.CurrentToken() == nil {
		return false
	}

	tok := l.lexStream.CurrentToken()

	_, isKeyword := sqlKeywords[strings.ToLower(tok.ValueString())]

	return !isKeyword && tok.Key() == tokenizer.TokenKeyword
}

func (l SQLLexer) EatKeyword(keyword string) error {
	ok, err := l.MatchKeyword(keyword)
	if !ok {
		return l.WrapLexerError(err)
	}

	l.nextToken()

	return nil
}

func (l SQLLexer) EatDelim(delim string) error {
	ok, err := l.MatchDelim(delim)
	if !ok {
		return l.WrapLexerError(err)
	}

	l.nextToken()

	return nil
}

func (l SQLLexer) EatIntConstant() (int64, error) {
	if !l.MatchIntConstant() {
		return 0, l.WrapLexerError(ErrBadSyntax)
	}

	val := l.lexStream.CurrentToken().ValueInt()

	l.nextToken()

	return val, nil
}

func (l SQLLexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", l.WrapLexerError(ErrBadSyntax)
	}

	id := l.lexStream.CurrentToken().ValueUnescapedString()

	l.nextToken()

	return id, nil
}

func (l SQLLexer) EatID() (string, error) {
	if !l.MatchID() {
		return "", l.WrapLexerError(ErrBadSyntax)
	}

	id := strings.ToLower(l.lexStream.CurrentToken().ValueString())

	l.nextToken()

	return id, nil
}

func (l SQLLexer) WrapLexerError(err error) error {
	tok := l.lexStream.CurrentToken()

	if tok.Key() == tokenizer.TokenUndef {
		return errors.WithMessage(err, `at end of query`)
	}

	snippet := l.lexStream.GetSnippet(2, 2) //nolint:gomnd
	snippetStrings := make([]string, len(snippet))

	for i := 0; i < len(snippet); i++ {
		snippetStrings[i] = snippet[i].ValueString()
	}

	return errors.WithMessagef(err, `near "%s" at line %d`, strings.Join(snippetStrings, " "), tok.Line())
}
