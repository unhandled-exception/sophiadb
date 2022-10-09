package parse

import (
	"strings"

	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

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
