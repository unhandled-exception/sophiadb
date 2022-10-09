package parse

import "github.com/pkg/errors"

type InsertStatement interface {
	Statement

	TableName() string
	Fields() FieldsList
	Values() ValuesList
}

type SQLInsertStatement struct {
	tableName string
	fields    FieldsList
	values    ValuesList
}

func NewSQLInsertStatement(q string) (*SQLInsertStatement, error) {
	lex := NewSQLLexer(q)

	stmt := new(SQLInsertStatement)
	err := stmt.Parse(lex)

	if errors.Is(err, ErrEOF) || (err == nil && !lex.EOF()) {
		return stmt, lex.WrapLexerError(ErrBadSyntax)
	}

	return stmt, err
}

func (s SQLInsertStatement) TableName() string {
	return s.tableName
}

func (s SQLInsertStatement) Fields() FieldsList {
	return s.fields
}

func (s SQLInsertStatement) Values() ValuesList {
	return s.values
}

func (s SQLInsertStatement) String() string {
	if len(s.fields) == 0 || len(s.values) == 0 {
		return ""
	}

	return "insert into " + s.TableName() + " (" + s.Fields().String() + ")" + " values (" + s.Values().String() + ")"
}

func (s *SQLInsertStatement) Parse(lex Lexer) error {
	s.tableName = ""
	s.fields = nil
	s.values = nil

	var err error

	if err = lex.EatKeyword("insert"); err != nil {
		switch {
		case errors.Is(err, ErrUnmatchedKeyword):
			return ErrInvalidStatement
		default:
			return err
		}
	}

	if err = lex.EatKeyword("into"); err != nil {
		return err
	}

	tableName, err := lex.EatID()
	if err != nil {
		return err
	}

	s.tableName = tableName

	if err = lex.EatDelim("("); err != nil {
		return err
	}

	fields := FieldsList{}
	if err = fields.Parse(lex); err != nil {
		return err
	}

	s.fields = fields

	if err = lex.EatDelim(")"); err != nil {
		return err
	}

	if err = lex.EatKeyword("values"); err != nil {
		return err
	}

	if err = lex.EatDelim("("); err != nil {
		return err
	}

	values := ValuesList{}
	if err = values.Parse(lex); err != nil {
		return err
	}

	s.values = values

	if err = lex.EatDelim(")"); err != nil {
		return err
	}

	return nil
}
