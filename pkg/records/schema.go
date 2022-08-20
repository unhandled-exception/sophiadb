package records

import (
	"fmt"
	"strings"

	"github.com/unhandled-exception/sophiadb/pkg/types"
)

type FieldType int

const (
	Int64Field FieldType = iota + 1
	StringField
	Int8Field
)

type FieldInfo struct {
	Type   FieldType
	Length int
}

func (fi FieldInfo) BytesLen() uint32 {
	switch fi.Type {
	case Int8Field:
		return types.PageInt8BytesLen()
	case Int64Field:
		return types.PageInt64BytesLen()
	case StringField:
		return types.PageStringBytesLen(fi.Length)
	default:
		return 0
	}
}

type Schema struct {
	fields []string
	info   map[string]FieldInfo
}

func NewSchema() Schema {
	return Schema{
		fields: make([]string, 0),
		info:   make(map[string]FieldInfo),
	}
}

func (s Schema) Count() int {
	return len(s.fields)
}

func (s Schema) HasField(name string) bool {
	_, ok := s.info[name]

	return ok
}

func (s Schema) Fields() []string {
	return s.fields
}

func (s Schema) Field(name string) (FieldInfo, bool) {
	field, ok := s.info[name]

	return field, ok
}

func (s *Schema) Type(name string) FieldType {
	var fieldType FieldType

	field, ok := s.info[name]
	if ok {
		fieldType = field.Type
	}

	return fieldType
}

func (s Schema) Length(name string) int {
	var fieldLen int

	field, ok := s.info[name]
	if ok {
		fieldLen = field.Length
	}

	return fieldLen
}

func (s *Schema) AddField(name string, t FieldType, length int) {
	s.fields = append(s.fields, name)
	s.info[name] = FieldInfo{
		Type:   t,
		Length: length,
	}
}

func (s *Schema) AddInt64Field(name string) {
	s.AddField(name, Int64Field, 0)
}

func (s *Schema) AddInt8Field(name string) {
	s.AddField(name, Int8Field, 0)
}

func (s *Schema) AddStringField(name string, length int) {
	s.AddField(name, StringField, length)
}

func (s *Schema) AddAll(schema Schema) {
	for _, name := range schema.Fields() {
		field, ok := schema.Field(name)
		if ok {
			s.AddField(name, field.Type, field.Length)
		}
	}
}

func (s Schema) String() string {
	fields := make([]string, 0, len(s.fields))
	for _, name := range s.fields {
		var str string

		field := s.info[name]

		switch field.Type {
		case Int8Field:
			str = fmt.Sprintf("[%s: int8]", name)
		case Int64Field:
			str = fmt.Sprintf("[%s: int64]", name)
		case StringField:
			str = fmt.Sprintf("[%s: string(%d)]", name, field.Length)
		}

		fields = append(fields, str)
	}

	return strings.Join(fields, ", ")
}
