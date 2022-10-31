package parse

import (
	"github.com/pkg/errors"

	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type SelectStatement interface {
	Statement

	Fields() FieldsList
	Tables() TablesList
	Pred() scan.Predicate
}

type SQLSelectStatement struct {
	fields FieldsList
	tables TablesList
	pred   scan.Predicate
}

func NewSQLSelectStatement(q string) (*SQLSelectStatement, error) {
	lex := NewSQLLexer(q)

	stmt := new(SQLSelectStatement)
	err := stmt.Parse(lex)

	if errors.Is(err, ErrEOF) || (err == nil && !lex.EOF()) {
		return stmt, lex.WrapLexerError(ErrBadSyntax)
	}

	return stmt, err
}

func (s SQLSelectStatement) String() string {
	if len(s.tables) == 0 || len(s.fields) == 0 {
		return ""
	}

	q := "select " + s.Fields().String() + " from " + s.Tables().String()

	if pred := s.Pred().String(); pred != "" {
		q += " where " + pred
	}

	return q
}

func (s SQLSelectStatement) Fields() FieldsList {
	return s.fields
}

func (s SQLSelectStatement) Tables() TablesList {
	return s.tables
}

func (s SQLSelectStatement) Pred() scan.Predicate {
	if s.pred == nil {
		return scan.NewAndPredicate()
	}

	return s.pred
}

func (s *SQLSelectStatement) Parse(lex Lexer) error {
	var err error

	s.fields = nil
	s.tables = nil
	s.pred = nil

	if err = lex.EatKeyword("select"); err != nil {
		return ErrInvalidStatement
	}

	fields := FieldsList{}
	if err = fields.Parse(lex); err != nil {
		return err
	}

	s.fields = fields

	err = lex.EatKeyword("from")
	if err != nil {
		return err
	}

	tables := TablesList{}
	if err = tables.Parse(lex); err != nil {
		return err
	}

	s.tables = tables

	switch ok, err := lex.MatchKeyword("where"); {
	case errors.Is(err, ErrEOF):
	case ok:
		_ = lex.EatKeyword("where")

		if s.pred, err = parsePredicate(lex); err != nil {
			return err
		}
	}

	return nil
}
