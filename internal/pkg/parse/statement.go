package parse

type Statement interface {
	Parse(Lexer) error
	String() string
}
