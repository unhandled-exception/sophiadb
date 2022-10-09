package scan

import (
	"strconv"

	"github.com/unhandled-exception/sophiadb/pkg/records"
)

type CompResult int8

var (
	CompUncomparable CompResult = -127
	CompLess         CompResult = -1
	CompEqual        CompResult = 0
	CompGreat        CompResult = 1
)

type Constant interface {
	Value() interface{}
	Type() records.FieldType
	CompareTo(Constant) CompResult
	String() string
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

func (c Int64Constant) String() string {
	return strconv.FormatInt(c.value, 10) //nolint:gomnd
}

func (c Int64Constant) CompareTo(another Constant) CompResult {
	var value int64

	switch another.Type() { //nolint:exhaustive
	case records.Int64Field:
		value, _ = another.Value().(int64)
	case records.Int8Field:
		value = int64(another.Value().(int8)) //nolint:forcetypeassert
	default:
		return CompUncomparable
	}

	if c.value < value {
		return CompLess
	}

	if c.value > value {
		return CompGreat
	}

	return CompEqual
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

func (c Int8Constant) String() string {
	return strconv.FormatInt(int64(c.value), 10) //nolint:gomnd
}

func (c Int8Constant) CompareTo(another Constant) CompResult {
	var value int8

	switch another.Type() { //nolint:exhaustive
	case records.Int8Field:
		value, _ = another.Value().(int8)
	case records.Int64Field:
		value = int8(another.Value().(int64)) //nolint:forcetypeassert
	default:
		return CompUncomparable
	}

	if c.value < value {
		return CompLess
	}

	if c.value > value {
		return CompGreat
	}

	return CompEqual
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

func (c StringConstant) String() string {
	return `'` + c.value + `'`
}

func (c StringConstant) CompareTo(another Constant) CompResult {
	var value string

	switch another.Type() { //nolint:exhaustive
	case records.StringField:
		value, _ = another.Value().(string)
	default:
		return CompUncomparable
	}

	if c.value < value {
		return CompLess
	}

	if c.value > value {
		return CompGreat
	}

	return CompEqual
}
