package scan

type Expression interface {
	Evaluate(s Scan) (Constant, error)
	String() string
}

type FieldExpression struct{}

func NewFieldExpression(fieldName string) FieldExpression {
	return FieldExpression{}
}

func (e FieldExpression) Evaluate(s Scan) (Constant, error) {
	panic("not implemented")
}

func (e FieldExpression) String() string {
	panic("not implemented")
}

type ScalarExpression struct{}

func NewScalarExpression(fieldName string) ScalarExpression {
	return ScalarExpression{}
}

func (e ScalarExpression) Evaluate(s Scan) (Constant, error) {
	panic("not implemented")
}

func (e ScalarExpression) String() string {
	panic("not implemented")
}
