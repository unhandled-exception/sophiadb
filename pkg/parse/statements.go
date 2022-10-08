package parse

type Statement interface {
	Parse(string) (interface{}, error)
	String() string
}

type QueryStatement struct{}

func (s QueryStatement) Parse(_ string) (interface{}, error) {
	panic("not implemented") // TODO: Implement
}

func (s QueryStatement) String() string {
	panic("not implemented") // TODO: Implement
}

type InsertStatement struct{}

func (s InsertStatement) Parse(_ string) (interface{}, error) {
	panic("not implemented") // TODO: Implement
}

func (s InsertStatement) String() string {
	panic("not implemented") // TODO: Implement
}

type DeleteStatement struct{}

func (s DeleteStatement) Parse(_ string) (interface{}, error) {
	panic("not implemented") // TODO: Implement
}

func (s DeleteStatement) String() string {
	panic("not implemented") // TODO: Implement
}

type UpdateStatement struct{}

func (s UpdateStatement) Parse(_ string) (interface{}, error) {
	panic("not implemented") // TODO: Implement
}

func (s UpdateStatement) String() string {
	panic("not implemented") // TODO: Implement
}

type CreateTableStatement struct{}

func (s CreateTableStatement) Parse(_ string) (interface{}, error) {
	panic("not implemented") // TODO: Implement
}

func (s CreateTableStatement) String() string {
	panic("not implemented") // TODO: Implement
}

type CreateViewStatement struct{}

func (s CreateViewStatement) Parse(_ string) (interface{}, error) {
	panic("not implemented") // TODO: Implement
}

func (s CreateViewStatement) String() string {
	panic("not implemented") // TODO: Implement
}
