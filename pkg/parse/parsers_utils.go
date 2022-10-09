package parse

import (
	"github.com/pkg/errors"

	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

func parseFields(lex Lexer) (FieldsList, error) {
	fieldName, err := lex.EatID()
	if err != nil {
		return nil, err
	}

	fields := FieldsList{fieldName}

	if ok, _ := lex.MatchDelim(","); ok {
		_ = lex.EatDelim(",")

		nextFields, werr := parseFields(lex)
		if werr != nil {
			return nil, werr
		}

		fields = append(fields, nextFields...)
	}

	return fields, nil
}

func parseTables(lex Lexer) (TablesList, error) {
	tableName, err := lex.EatID()
	if err != nil {
		return nil, err
	}

	tables := TablesList{tableName}

	if ok, _ := lex.MatchDelim(","); ok {
		_ = lex.EatDelim(",")

		nextTables, werr := parseTables(lex)
		if werr != nil {
			return nil, werr
		}

		tables = append(tables, nextTables...)
	}

	return tables, nil
}

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

	ok, err := lex.MatchKeyword("and")
	if err != nil && !errors.Is(err, ErrEOF) {
		return nil, err
	}

	if ok {
		_ = lex.EatKeyword("and")

		nextPred, err := parsePredicate(lex)
		if err != nil {
			return nil, err
		}

		pred.ConjoinWith(nextPred)
	}

	return pred, nil
}

func parseValues(lex Lexer) (ValuesList, error) {
	value, err := parseConstant(lex)
	if err != nil {
		return nil, err
	}

	values := ValuesList{value}

	if ok, _ := lex.MatchDelim(","); ok {
		_ = lex.EatDelim(",")

		nextValues, werr := parseValues(lex)
		if werr != nil {
			return nil, werr
		}

		values = append(values, nextValues...)
	}

	return values, nil
}
