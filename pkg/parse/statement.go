package parse

type Statement interface {
	Parse(string) error
	String() string
}
