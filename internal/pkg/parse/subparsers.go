package parse

import (
	"strings"

	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

func parseConstant(lex Lexer) (scan.Constant, error) {
	if lex.MatchIntConstant() {
		value, _ := lex.EatIntConstant()

		if value >= -127 && value <= 127 {
			return scan.NewInt8Constant(int8(value)), nil
		}

		return scan.NewInt64Constant(value), nil
	}

	if lex.MatchStringConstant() {
		value, _ := lex.EatStringConstant()

		return scan.NewStringConstant(value), nil
	}

	return nil, lex.WrapLexerError(ErrBadSyntax)
}

func parseExpression(lex Lexer) (scan.Expression, error) {
	if lex.MatchID() {
		fieldName, _ := lex.EatID()

		return scan.NewFieldExpression(fieldName), nil
	}

	valueConst, err := parseConstant(lex)
	if err != nil {
		return nil, err
	}

	return scan.NewScalarExpression(valueConst), nil
}

func parseAndTerm(lex Lexer) (scan.Term, error) {
	lhs, err := parseExpression(lex)
	if err != nil {
		return nil, err
	}

	err = lex.EatDelim("=")
	if err != nil {
		return nil, err
	}

	rhs, err := parseExpression(lex)
	if err != nil {
		return nil, err
	}

	return scan.NewEqualTerm(lhs, rhs), nil
}

func parsePredicate(lex Lexer) (scan.Predicate, error) {
	term, err := parseAndTerm(lex)
	if err != nil {
		return nil, err
	}

	pred := scan.NewAndPredicate(term)

	if ok, _ := lex.MatchKeyword("and"); ok {
		_ = lex.EatKeyword("and")

		nextPred, err := parsePredicate(lex)
		if err != nil {
			return nil, err
		}

		pred.ConjoinWith(nextPred)
	}

	return pred, nil
}

type FieldsList []string

func (f FieldsList) String() string {
	return strings.Join(f, ", ")
}

func (f *FieldsList) Parse(lex Lexer) error {
	var err error

	fieldName, err := lex.EatID()
	if err != nil {
		return err
	}

	*f = append(*f, fieldName)

	if ok, _ := lex.MatchDelim(","); ok {
		_ = lex.EatDelim(",")

		nextFields := FieldsList{}
		if err = nextFields.Parse(lex); err != nil {
			return err
		}

		*f = append(*f, nextFields...)
	}

	return nil
}

type TablesList []string

func (t TablesList) String() string {
	return strings.Join(t, ", ")
}

func (t *TablesList) Parse(lex Lexer) error {
	var err error

	tableName, err := lex.EatID()
	if err != nil {
		return err
	}

	*t = append(*t, tableName)

	if ok, _ := lex.MatchDelim(","); ok {
		_ = lex.EatDelim(",")

		nextTables := TablesList{}
		if err = nextTables.Parse(lex); err != nil {
			return err
		}

		*t = append(*t, nextTables...)
	}

	return nil
}

type ValuesList []scan.Constant

func (c ValuesList) String() string {
	l := make([]string, len(c))

	for i := 0; i < len(c); i++ {
		l[i] = c[i].String()
	}

	return strings.Join(l, ", ")
}

func (v *ValuesList) Parse(lex Lexer) error {
	var err error

	value, err := parseConstant(lex)
	if err != nil {
		return err
	}

	*v = append(*v, value)

	if ok, _ := lex.MatchDelim(","); ok {
		_ = lex.EatDelim(",")

		nextValues := ValuesList{}
		if err = nextValues.Parse(lex); err != nil {
			return err
		}

		*v = append(*v, nextValues...)
	}

	return nil
}
