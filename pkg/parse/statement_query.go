package parse

import (
	"strings"

	"github.com/pkg/errors"

	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type SelectStatement interface {
	Statement

	Fields() []string
	Tables() []string
	Pred() scan.Predicate
}

type SQLSelectStatement struct {
	fields []string
	tables []string
	pred   scan.Predicate
}

func (s SQLSelectStatement) String() string {
	if len(s.tables) == 0 || len(s.fields) == 0 {
		return ""
	}

	q := "select " +
		strings.Join(s.Fields(), ", ") +
		" from " +
		strings.Join(s.Tables(), ", ")

	if pred := s.Pred(); pred != nil {
		q += " where " + pred.String()
	}

	return q
}

func (s SQLSelectStatement) Fields() []string {
	return s.fields
}

func (s SQLSelectStatement) Tables() []string {
	return s.tables
}

func (s SQLSelectStatement) Pred() scan.Predicate {
	return s.pred
}

func (s *SQLSelectStatement) Parse(q string) error {
	lex := NewSQLLexer(q)

	err := func() error {
		s.fields = nil
		s.tables = nil
		s.pred = nil

		if err := lex.EatKeyword("select"); err != nil {
			switch {
			case errors.Is(err, ErrUnmatchedKeyword):
				return ErrInvalidStatement
			default:
				return err
			}
		}

		fields, err := parseFields(lex)
		if err != nil {
			return err
		}

		s.fields = fields

		err = lex.EatKeyword("from")
		if err != nil {
			return err
		}

		tables, err := parseTables(lex)
		if err != nil {
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
		default:
			return err
		}

		return nil
	}()

	switch {
	case errors.Is(err, ErrEOF):
		return lex.WrapLexerError(ErrBadSyntax)
	default:
		return err
	}
}
