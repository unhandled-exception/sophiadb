package parse

import (
	"fmt"

	"github.com/pkg/errors"
)

type CreateIndexStatement interface {
	Statement

	IndexName() string
	TableName() string
	Fields() FieldsList
}

type SQLCreateIndexStatement struct {
	indexName string
	tableName string
	fields    FieldsList
}

func NewSQLCreateIndexStatement(q string) (*SQLCreateIndexStatement, error) {
	lex := NewSQLLexer(q)

	stmt := new(SQLCreateIndexStatement)
	err := stmt.Parse(lex)

	if errors.Is(err, ErrEOF) || (err == nil && !lex.EOF()) {
		return stmt, lex.WrapLexerError(ErrBadSyntax)
	}

	return stmt, err
}

func (s SQLCreateIndexStatement) String() string {
	if s.IndexName() == "" || s.TableName() == "" || len(s.Fields()) == 0 {
		return ""
	}

	q := fmt.Sprintf(
		"create index %s on %s (%s)",
		s.IndexName(),
		s.TableName(),
		s.Fields().String(),
	)

	return q
}

func (s SQLCreateIndexStatement) IndexName() string {
	return s.indexName
}

func (s SQLCreateIndexStatement) TableName() string {
	return s.tableName
}

func (s SQLCreateIndexStatement) Fields() FieldsList {
	return s.fields
}

func (s *SQLCreateIndexStatement) Parse(lex Lexer) error {
	s.indexName = ""
	s.tableName = ""
	s.fields = nil

	var err error

	if err = lex.EatKeyword("create"); err != nil {
		switch {
		case errors.Is(err, ErrUnmatchedKeyword):
			return ErrInvalidStatement
		default:
			return err
		}
	}

	if err = lex.EatKeyword("index"); err != nil {
		switch {
		case errors.Is(err, ErrUnmatchedKeyword):
			return ErrInvalidStatement
		default:
			return err
		}
	}

	indexName, err := lex.EatID()
	if err != nil {
		return err
	}

	s.indexName = indexName

	if err = lex.EatKeyword("on"); err != nil {
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

	fields, err := parseFields(lex)
	if err != nil {
		return err
	}

	s.fields = fields

	if err = lex.EatDelim(")"); err != nil {
		return err
	}

	return nil
}
