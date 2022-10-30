package parse

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type DeleteStatement interface {
	Statement

	TableName() string
	Pred() scan.Predicate
}

type SQLDeleteStatement struct {
	tableName string
	pred      scan.Predicate
}

func NewSQLDeleteStatement(q string) (*SQLDeleteStatement, error) {
	lex := NewSQLLexer(q)

	stmt := new(SQLDeleteStatement)
	err := stmt.Parse(lex)

	if errors.Is(err, ErrEOF) || (err == nil && !lex.EOF()) {
		return stmt, lex.WrapLexerError(ErrBadSyntax)
	}

	return stmt, err
}

func (s SQLDeleteStatement) String() string {
	if s.tableName == "" {
		return ""
	}

	q := "delete from " + s.TableName()

	if pred := s.Pred().String(); pred != "" {
		q += " where " + pred
	}

	return q
}

func (s SQLDeleteStatement) TableName() string {
	return s.tableName
}

func (s SQLDeleteStatement) Pred() scan.Predicate {
	if s.pred == nil {
		return scan.NewAndPredicate()
	}

	return s.pred
}

func (s *SQLDeleteStatement) Parse(lex Lexer) error {
	s.tableName = ""
	s.pred = nil

	if err := lex.EatKeyword("delete"); err != nil {
		return ErrInvalidStatement
	}

	var err error

	err = lex.EatKeyword("from")
	if err != nil {
		return err
	}

	tableName, err := lex.EatID()
	if err != nil {
		return err
	}

	s.tableName = tableName

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
