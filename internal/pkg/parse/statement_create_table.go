package parse

import (
	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
)

type CreateTableStatement interface {
	Statement

	TableName() string
	Schema() records.Schema
}

type SQLCreateTableStatement struct {
	tableName string
	schema    records.Schema
}

func NewSQLCreateTableStatement(q string) (*SQLCreateTableStatement, error) {
	lex := NewSQLLexer(q)

	stmt := new(SQLCreateTableStatement)
	err := stmt.Parse(lex)

	if errors.Is(err, ErrEOF) || (err == nil && !lex.EOF()) {
		return stmt, lex.WrapLexerError(ErrBadSyntax)
	}

	return stmt, err
}

func (s SQLCreateTableStatement) String() string {
	if s.TableName() == "" || s.Schema().Count() == 0 {
		return ""
	}

	q := "create table " + s.TableName() + " (" + s.Schema().String() + ")"

	return q
}

func (s SQLCreateTableStatement) TableName() string {
	return s.tableName
}

func (s SQLCreateTableStatement) Schema() records.Schema {
	return s.schema
}

func (s *SQLCreateTableStatement) Parse(lex Lexer) error {
	var err error

	s.tableName = ""
	s.schema = records.NewSchema()

	if err = lex.EatKeyword("create"); err != nil {
		return ErrInvalidStatement
	}

	if err = lex.EatKeyword("table"); err != nil {
		return ErrInvalidStatement
	}

	tableName, err := lex.EatID()
	if err != nil {
		return err
	}

	s.tableName = tableName

	if err = lex.EatDelim("("); err != nil {
		return err
	}

	schema, err := s.parseSchema(lex)
	if err != nil {
		return err
	}

	s.schema = schema

	if err = lex.EatDelim(")"); err != nil {
		return err
	}

	return nil
}

func (s SQLCreateTableStatement) parseSchema(lex Lexer) (records.Schema, error) {
	var err error

	schema := records.NewSchema()

	fieldName, err := lex.EatID()
	if err != nil {
		return schema, err
	}

	switch {
	case lex.EatKeyword("int8") == nil:
		schema.AddInt8Field(fieldName)
	case lex.EatKeyword("int64") == nil:
		schema.AddInt64Field(fieldName)
	case lex.EatKeyword("varchar") == nil:
		if err = lex.EatDelim("("); err != nil {
			return schema, err
		}

		length, err := lex.EatIntConstant()
		if err != nil {
			return schema, err
		}

		if err = lex.EatDelim(")"); err != nil {
			return schema, err
		}

		schema.AddStringField(fieldName, length)
	default:
		return schema, lex.WrapLexerError(ErrBadSyntax)
	}

	if ok, _ := lex.MatchDelim(","); ok {
		_ = lex.EatDelim(",")

		nextDefs, err := s.parseSchema(lex)
		if err != nil {
			return schema, err
		}

		schema.AddAll(nextDefs)
	}

	return schema, nil
}
