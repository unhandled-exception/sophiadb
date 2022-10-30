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
	lexStream *SQLTokenizer
}

func NewSQLLexer(text string) SQLLexer {
	l := SQLLexer{
		lexStream: NewSQLtokenizer(text),
	}

	l.lexStream.NextToken()

	return l
}

func (l SQLLexer) Close() {
}

func (l SQLLexer) nextToken() {
	l.lexStream.NextToken()
}

func (l SQLLexer) EOF() bool {
	return l.lexStream.CurrentToken().Typ == TokEOF
}

func (l SQLLexer) MatchKeyword(keyword string) (bool, error) {
	tok := l.lexStream.CurrentToken()
	switch tok.Typ {
	case TokEOF:
		return false, ErrEOF
	case TokError:
		return false, fmt.Errorf("%s", tok.Val)
	}

	if tok.Typ != TokKeyword {
		return false, ErrBadSyntax
	}

	if tok.Val != strings.ToLower(keyword) {
		return false, ErrUnmatchedKeyword
	}

	return true, nil
}

func (l SQLLexer) MatchDelim(delim string) (bool, error) {
	tok := l.lexStream.CurrentToken()
	switch tok.Typ {
	case TokEOF:
		return false, ErrEOF
	case TokError:
		return false, fmt.Errorf("%s", tok.Val)
	}

	var err error

	if tok.Val != delim {
		err = ErrUnmatchedDelim
	}

	return err == nil, err
}

func (l SQLLexer) MatchIntConstant() bool {
	tok := l.lexStream.CurrentToken()
	switch tok.Typ {
	case TokEOF:
		return false
	case TokError:
		return false
	}

	return tok.Typ == TokNumber
}

func (l SQLLexer) MatchStringConstant() bool {
	tok := l.lexStream.CurrentToken()
	switch tok.Typ {
	case TokEOF:
		return false
	case TokError:
		return false
	}

	return tok.Typ == TokString
}

func (l SQLLexer) MatchID() bool {
	tok := l.lexStream.CurrentToken()
	switch tok.Typ {
	case TokEOF:
		return false
	case TokError:
		return false
	}

	return tok.Typ == TokIdentifier
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

	val, err := strconv.ParseInt(l.lexStream.CurrentToken().Val, 10, 64)
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
		l.lexStream.CurrentToken().Val,
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

	id := strings.ToLower(l.lexStream.CurrentToken().Val)

	l.nextToken()

	return id, nil
}

func (l SQLLexer) WrapLexerError(err error) error {
	return errors.WithMessagef(err, `near "%s" at line %d`, l.lexStream.Input[:l.lexStream.Pos], l.lexStream.Line)
}
