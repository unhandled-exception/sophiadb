package scan

import (
	"math"

	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
)

type ExpressionValue int8

const (
	StringValue ExpressionValue = iota
	ConstantValue
)

type Expression interface {
	Evaluate(s Scan) (Constant, error)
	IsFieldName() bool
	Value() (any, ExpressionValue)
	AppliesTo(records.Schema) bool
	ReductionFactor(Plan) (int64, bool)
	String() string
}

type FieldExpression struct {
	fieldName string
}

func NewFieldExpression(fieldName string) FieldExpression {
	return FieldExpression{
		fieldName: fieldName,
	}
}

func (e FieldExpression) Value() (any, ExpressionValue) {
	return e.fieldName, StringValue
}

func (e FieldExpression) Evaluate(s Scan) (Constant, error) {
	return s.GetVal(e.fieldName)
}

func (e FieldExpression) IsFieldName() bool {
	return true
}

func (e FieldExpression) AppliesTo(s records.Schema) bool {
	return s.HasField(e.fieldName)
}

func (e FieldExpression) ReductionFactor(p Plan) (int64, bool) {
	return p.DistinctValues(e.fieldName)
}

func (e FieldExpression) String() string {
	return e.fieldName
}

type ScalarExpression struct {
	value Constant
}

func NewScalarExpression(value Constant) ScalarExpression {
	return ScalarExpression{
		value: value,
	}
}

func (e ScalarExpression) Value() (any, ExpressionValue) {
	return e.value, ConstantValue
}

func (e ScalarExpression) Evaluate(s Scan) (Constant, error) {
	return e.value, nil
}

func (e ScalarExpression) String() string {
	return e.value.String()
}

func (e ScalarExpression) IsFieldName() bool {
	return false
}

func (e ScalarExpression) AppliesTo(s records.Schema) bool {
	return true
}

func (e ScalarExpression) ReductionFactor(p Plan) (int64, bool) {
	return math.MaxInt64, true
}
