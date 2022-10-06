package parse

type Lexer interface {
	MatchDelim(rune) bool
	MatchIntConstant() bool
	MatchStringConstant() bool
	MatchKeyword(keyword string) bool
	MatchID() bool

	EatDelim(rune) bool
	EatIntConstant() (bool, int64)
	EatStringConstant() (bool, string)
	EatKeyword(keyword string) bool
	EatID() (bool, string)
}

type SQLLexer struct {
	stream string
}

func NewLexer(stream string) SQLLexer {
	l := SQLLexer{
		stream: stream,
	}

	return l
}

func (l SQLLexer) MatchDelim(_ rune) bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) MatchIntConstant() bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) MatchStringConstant() bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) MatchKeyword(keyword string) bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) MatchID() bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) EatDelim(_ rune) bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) EatIntConstant() (bool, int64) {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) EatStringConstant() (bool, string) {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) EatKeyword(keyword string) bool {
	panic("not implemented") // TODO: Implement
}

func (l SQLLexer) EatID() (bool, string) {
	panic("not implemented") // TODO: Implement
}
