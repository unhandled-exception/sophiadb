package parse

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type UpdateStatement interface {
	Statement

	TableName() string
	UpdateExpressions() UpdateExpressions
	Pred() scan.Predicate
}

type SQLUpdateStatement struct {
	tableName         string
	updateExpressions UpdateExpressions
	pred              scan.Predicate
}

func NewSQLUpdateStatement(q string) (*SQLUpdateStatement, error) {
	lex := NewSQLLexer(q)

	stmt := new(SQLUpdateStatement)
	err := stmt.Parse(lex)

	if errors.Is(err, ErrEOF) || (err == nil && !lex.EOF()) {
		return stmt, lex.WrapLexerError(ErrBadSyntax)
	}

	return stmt, err
}

func (s SQLUpdateStatement) String() string {
	if s.TableName() == "" || len(s.UpdateExpressions()) == 0 {
		return ""
	}

	q := "update " + s.TableName() + " set " + s.updateExpressions.String()

	if pred := s.Pred().String(); pred != "" {
		q += " where " + pred
	}

	return q
}

func (s SQLUpdateStatement) TableName() string {
	return s.tableName
}

func (s SQLUpdateStatement) UpdateExpressions() UpdateExpressions {
	return s.updateExpressions
}

func (s SQLUpdateStatement) Pred() scan.Predicate {
	if s.pred == nil {
		return scan.NewAndPredicate()
	}

	return s.pred
}

func (s *SQLUpdateStatement) Parse(lex Lexer) error {
	s.tableName = ""
	s.pred = nil

	var err error

	if err = lex.EatKeyword("update"); err != nil {
		return ErrInvalidStatement
	}

	tableName, err := lex.EatID()
	if err != nil {
		return err
	}

	s.tableName = tableName

	err = lex.EatKeyword("set")
	if err != nil {
		return err
	}

	updateExpressions := UpdateExpressions{}

	if err = updateExpressions.Parse(lex); err != nil {
		return err
	}

	s.updateExpressions = updateExpressions

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

type FieldExpression struct {
	FieldName string
	Value     scan.Constant
}

func (e FieldExpression) String() string {
	if e.FieldName == "" || e.Value == nil {
		return ""
	}

	return e.FieldName + " = " + e.Value.String()
}

func (e *FieldExpression) Parse(lex Lexer) error {
	var err error

	fieldName, err := lex.EatID()
	if err != nil {
		return err
	}

	e.FieldName = fieldName

	if err = lex.EatDelim("="); err != nil {
		return err
	}

	value, err := parseConstant(lex)
	if err != nil {
		return err
	}

	e.Value = value

	return nil
}

type UpdateExpressions []FieldExpression

func (u UpdateExpressions) String() string {
	e := make([]string, len(u))

	for i := 0; i < len(u); i++ {
		e[i] = u[i].String()
	}

	return strings.Join(e, ", ")
}

func (u *UpdateExpressions) Parse(lex Lexer) error {
	var err error

	expr := FieldExpression{}
	if err = expr.Parse(lex); err != nil {
		return err
	}

	*u = append(*u, expr)

	if ok, _ := lex.MatchDelim(","); ok {
		_ = lex.EatDelim(",")

		nextValues := UpdateExpressions{}
		if err = nextValues.Parse(lex); err != nil {
			return err
		}

		*u = append(*u, nextValues...)
	}

	return nil
}
