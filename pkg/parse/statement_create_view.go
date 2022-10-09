package parse

import (
	"github.com/pkg/errors"
)

type CreateViewStatement interface {
	Statement

	ViewName() string
	Query() SelectStatement
}

type SQLCreateViewStatement struct {
	viewName string
	query    SelectStatement
}

func NewSQLCreateViewStatement(q string) (*SQLCreateViewStatement, error) {
	lex := NewSQLLexer(q)

	stmt := new(SQLCreateViewStatement)
	err := stmt.Parse(lex)

	if errors.Is(err, ErrEOF) || (err == nil && !lex.EOF()) {
		return stmt, lex.WrapLexerError(ErrBadSyntax)
	}

	return stmt, err
}

func (s SQLCreateViewStatement) String() string {
	if s.ViewName() == "" || s.Query() == nil {
		return ""
	}

	q := "create view " + s.ViewName() + " as " + s.Query().String()

	return q
}

func (s SQLCreateViewStatement) ViewName() string {
	return s.viewName
}

func (s SQLCreateViewStatement) Query() SelectStatement {
	return s.query
}

func (s *SQLCreateViewStatement) Parse(lex Lexer) error {
	s.viewName = ""
	s.query = nil

	var err error

	if err = lex.EatKeyword("create"); err != nil {
		switch {
		case errors.Is(err, ErrUnmatchedKeyword):
			return ErrInvalidStatement
		default:
			return err
		}
	}

	if err = lex.EatKeyword("view"); err != nil {
		switch {
		case errors.Is(err, ErrUnmatchedKeyword):
			return ErrInvalidStatement
		default:
			return err
		}
	}

	viewName, err := lex.EatID()
	if err != nil {
		return err
	}

	s.viewName = viewName

	if err = lex.EatKeyword("as"); err != nil {
		return err
	}

	query := &SQLSelectStatement{}

	if err = query.Parse(lex); err != nil {
		return lex.WrapLexerError(ErrBadSyntax)
	}

	s.query = query

	return nil
}
