//nolint:exhaustive
package parse

import (
	"fmt"
	"strconv"
	"strings"

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

type SQLLexer struct {
	lexStream *sqlTokenizer
}

func NewSQLLexer(text string) SQLLexer {
	l := SQLLexer{
		lexStream: newSQLtokenizer(text),
	}

	l.lexStream.nextToken()

	return l
}

func (l SQLLexer) Close() {
}

func (l SQLLexer) nextToken() {
	l.lexStream.nextToken()
}

func (l SQLLexer) EOF() bool {
	return l.lexStream.currentToken().typ == tokEOF
}

func (l SQLLexer) MatchKeyword(keyword string) (bool, error) {
	tok := l.lexStream.currentToken()
	switch tok.typ {
	case tokEOF:
		return false, ErrEOF
	case tokError:
		return false, fmt.Errorf("%s", tok.val)
	}

	if tok.typ != tokKeyword {
		return false, ErrBadSyntax
	}

	if tok.val != strings.ToLower(keyword) {
		return false, ErrUnmatchedKeyword
	}

	return true, nil
}

func (l SQLLexer) MatchDelim(delim string) (bool, error) {
	tok := l.lexStream.currentToken()
	switch tok.typ {
	case tokEOF:
		return false, ErrEOF
	case tokError:
		return false, fmt.Errorf("%s", tok.val)
	}

	var err error

	if tok.val != delim {
		err = ErrUnmatchedDelim
	}

	return err == nil, err
}

func (l SQLLexer) MatchIntConstant() bool {
	tok := l.lexStream.currentToken()
	switch tok.typ {
	case tokEOF:
		return false
	case tokError:
		return false
	}

	return tok.typ == tokNumber
}

func (l SQLLexer) MatchStringConstant() bool {
	tok := l.lexStream.currentToken()
	switch tok.typ {
	case tokEOF:
		return false
	case tokError:
		return false
	}

	return tok.typ == tokString
}

func (l SQLLexer) MatchID() bool {
	tok := l.lexStream.currentToken()
	switch tok.typ {
	case tokEOF:
		return false
	case tokError:
		return false
	}

	return tok.typ == tokIdentifier
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

	val, err := strconv.ParseInt(l.lexStream.currentToken().val, 10, 64)
	if err != nil {
		return 0, errors.WithMessage(ErrBadSyntax, err.Error())
	}

	l.nextToken()

	return val, nil
}

func (l SQLLexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", l.WrapLexerError(ErrBadSyntax)
	}

	val := l.unescapeValueString(
		l.lexStream.currentToken().val,
	)

	l.nextToken()

	return val, nil
}

func (l SQLLexer) unescapeValueString(s string) string {
	s = strings.TrimPrefix(s, "'")
	s = strings.TrimSuffix(s, "'")

	if len(s) > 0 {
		res := make([]byte, 0, len(s))

		escaped := false

		for _, b := range []byte(s) {
			if escaped {
				switch b {
				case 'n':
					b = '\n'
				case 't':
					b = '\t'
				case '\\':
					b = '\\'
				}

				res = append(res, b)
				escaped = false

				continue
			}

			if b == '\\' {
				escaped = true

				continue
			}

			res = append(res, b)
		}

		s = string(res)
	}

	return s
}

func (l SQLLexer) EatID() (string, error) {
	if !l.MatchID() {
		return "", l.WrapLexerError(ErrBadSyntax)
	}

	id := strings.ToLower(l.lexStream.currentToken().val)

	l.nextToken()

	return id, nil
}

func (l SQLLexer) WrapLexerError(err error) error { //nolint:wsl
	// TODO: make new wrapper
	// tok := l.lexStream.currentToken()

	// if tok.typ == tokUndef {
	// 	return errors.WithMessage(err, `at end of query`)
	// }

	// snippet := l.lexStream.GetSnippet(2, 2) //nolint:gomnd
	// snippetStrings := make([]string, len(snippet))

	// for i := 0; i < len(snippet); i++ {
	// 	snippetStrings[i] = snippet[i].ValueString()
	// }

	// return errors.WithMessagef(err, `near "%s" at line %d`, strings.Join(snippetStrings, " "), tok.Line())

	return err
}
