package scan

import "github.com/unhandled-exception/sophiadb/pkg/records"

type Constant interface {
	Value() interface{}
	Type() records.FieldType
	// CompareTo(Constant) (int, bool)
	// EqualsTo(interface{}) (bool, bool)
	// String() string
}

func NewInt64Constant(value int64) Int64Constant {
	return Int64Constant{
		vType: records.Int64Field,
		value: value,
	}
}

func NewInt8Constant(value int8) Int8Constant {
	return Int8Constant{
		vType: records.Int8Field,
		value: value,
	}
}

func NewStringConstant(value string) StringConstant {
	return StringConstant{
		vType: records.StringField,
		value: value,
	}
}

type Int64Constant struct {
	value int64
	vType records.FieldType
}

func (c Int64Constant) Value() interface{} {
	return c.value
}

func (c Int64Constant) Type() records.FieldType {
	return c.vType
}

type Int8Constant struct {
	value int8
	vType records.FieldType
}

func (c Int8Constant) Value() interface{} {
	return c.value
}

func (c Int8Constant) Type() records.FieldType {
	return c.vType
}

type StringConstant struct {
	value string
	vType records.FieldType
}

func (c StringConstant) Value() interface{} {
	return c.value
}

func (c StringConstant) Type() records.FieldType {
	return c.vType
}
