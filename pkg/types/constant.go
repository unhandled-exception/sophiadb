package types

// TODO: Доделать при программировании образов сканирования

type Constant interface {
	Value() interface{}
}

func NewInt64Constant(value int64) Constant {
	return Int64Constant{value: value}
}

func NewInt8Constant(value int8) Constant {
	return Int8Constant{value: value}
}

func NewStringConstant(value string) Constant {
	return StringConstant{value: value}
}

type Int64Constant struct {
	value int64
}

func (c Int64Constant) Value() interface{} {
	return c.value
}

type Int8Constant struct {
	value int8
}

func (c Int8Constant) Value() interface{} {
	return c.value
}

type StringConstant struct {
	value string
}

func (c StringConstant) Value() interface{} {
	return c.value
}
