package parse

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
)

type CreateIndexStatement interface {
	Statement

	IndexName() string
	TableName() string
	Fields() FieldsList
	IndexType() indexes.IndexType
}

const defaultIndexType = indexes.HashIndexType

type SQLCreateIndexStatement struct {
	indexName string
	tableName string
	fields    FieldsList
	indexType indexes.IndexType
}

func NewSQLCreateIndexStatement(q string) (*SQLCreateIndexStatement, error) {
	lex := NewSQLLexer(q)

	stmt := &SQLCreateIndexStatement{
		indexType: defaultIndexType,
	}

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
		"create index %s on %s (%s) using %s",
		s.IndexName(),
		s.TableName(),
		s.Fields().String(),
		indexes.IndexTypeNames[s.indexType],
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

func (s SQLCreateIndexStatement) IndexType() indexes.IndexType {
	return s.indexType
}

func (s *SQLCreateIndexStatement) Parse(lex Lexer) error {
	s.indexName = ""
	s.tableName = ""
	s.fields = nil

	var err error

	if err = lex.EatKeyword("create"); err != nil {
		return ErrInvalidStatement
	}

	if err = lex.EatKeyword("index"); err != nil {
		return ErrInvalidStatement
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
		return lex.WrapLexerError(err)
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

	if ok, _ := lex.MatchKeyword("using"); ok {
		_ = lex.EatKeyword("using")

		it, err := lex.EatID()
		if err != nil {
			return err
		}

		it = strings.ToLower(it)

		for indexType, indexTypeName := range indexes.IndexTypeNames {
			if it == indexTypeName {
				s.indexType = indexType

				return nil
			}
		}

		return lex.WrapLexerError(ErrBadSyntax)
	}

	return nil
}
