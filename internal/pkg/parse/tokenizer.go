package parse

// Токенайзер по идеи лексера из доклада Роба Пайка — https://www.youtube.com/watch?v=HxaD_trXwRE&t=1567
// и кода из https://cs.opensource.google/go/go/+/master:src/text/template/parse/lex.go

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Pos int

type tokenType int

const eof = -1

const (
	TokError tokenType = iota
	TokEOF
	TokKeyword
	TokIdentifier
	TokString
	TokNumber
	TokDelimiter
)

var reservedIdentifiers = map[string]tokenType{
	"select":  TokKeyword,
	"from":    TokKeyword,
	"where":   TokKeyword,
	"and":     TokKeyword,
	"insert":  TokKeyword,
	"into":    TokKeyword,
	"values":  TokKeyword,
	"delete":  TokKeyword,
	"update":  TokKeyword,
	"set":     TokKeyword,
	"create":  TokKeyword,
	"table":   TokKeyword,
	"varchar": TokKeyword,
	"int":     TokKeyword,
	"int64":   TokKeyword,
	"int8":    TokKeyword,
	"view":    TokKeyword,
	"as":      TokKeyword,
	"index":   TokKeyword,
	"on":      TokKeyword,
	"using":   TokKeyword,
}

type Token struct {
	Typ  tokenType
	Pos  Pos
	Val  string
	Line int
}

func (i Token) String() string {
	switch {
	case i.Typ == TokEOF:
		return "{EOF}"
	case i.Typ == TokError:
		return fmt.Sprintf("/%s/", i.Val)
	case i.Typ == TokKeyword:
		return fmt.Sprintf("<%s>", i.Val)
	case i.Typ == TokIdentifier:
		return fmt.Sprintf("[%s]", i.Val)
	}

	return i.Val
}

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*SQLTokenizer) stateFn

// SQLTokenizer holds the state of the scanner.
type SQLTokenizer struct {
	Input string // the string being scanned
	Pos   Pos    // current position in the input
	Line  int    // 1+number of newlines seen

	start     Pos   // start position of this item
	startLine int   // start line of this item
	atEOF     bool  // we have hit the end of input and returned eof
	token     Token // item to return to parser
}

// NewSQLTokenizer create a new tokenizer object
func NewSQLtokenizer(input string) *SQLTokenizer {
	l := &SQLTokenizer{
		Input:     input,
		Line:      1,
		startLine: 1,
	}

	return l
}

// CurrentToken returns the current token
func (l *SQLTokenizer) CurrentToken() Token {
	return l.token
}

// NextToken returns the next item from the input.
// Called by the parser, not in the lexing goroutine.
func (l *SQLTokenizer) NextToken() {
	l.token = Token{TokEOF, l.Pos, "EOF", l.startLine}

	state := lexSQL

	for {
		state = state(l)
		if state == nil {
			return
		}
	}
}

// nextRune returns the nextRune rune in the input.
func (l *SQLTokenizer) nextRune() rune {
	if int(l.Pos) >= len(l.Input) {
		l.atEOF = true

		return eof
	}

	r, w := utf8.DecodeRuneInString(l.Input[l.Pos:])
	l.Pos += Pos(w)

	if r == '\n' {
		l.Line++
	}

	return r
}

// peek returns but does not consume the next rune in the input.
func (l *SQLTokenizer) peekRune() rune {
	r := l.nextRune()
	l.backup()

	return r
}

// backup steps back one rune.
func (l *SQLTokenizer) backup() {
	if !l.atEOF && l.Pos > 0 {
		r, w := utf8.DecodeLastRuneInString(l.Input[:l.Pos])
		l.Pos -= Pos(w)
		// Correct newline count.
		if r == '\n' {
			l.Line--
		}
	}
}

// thisToken returns the item at the current input point with the specified type
// and advances the input.
func (l *SQLTokenizer) thisToken(t tokenType) Token {
	i := Token{t, l.start, l.Input[l.start:l.Pos], l.startLine}
	l.start = l.Pos
	l.startLine = l.Line

	return i
}

// emit passes the trailing text as an item back to the parser.
func (l *SQLTokenizer) emit(t tokenType) stateFn {
	return l.emitToken(l.thisToken(t))
}

// emit passes the trailing text as an item back to the parser.
func (l *SQLTokenizer) emitIdentifier() stateFn {
	tok := l.thisToken(TokIdentifier)

	keyword := strings.ToLower(tok.Val)
	if typ, ok := reservedIdentifiers[keyword]; ok {
		tok.Typ = typ
		tok.Val = keyword
	}

	return l.emitToken(tok)
}

// emitToken passes the specified item to the parser.
func (l *SQLTokenizer) emitToken(i Token) stateFn {
	l.token = i

	return nil
}

// accept consumes the next rune if it's from the valid set.
func (l *SQLTokenizer) accept(valid string) bool {
	if strings.ContainsRune(valid, l.nextRune()) {
		return true
	}

	l.backup()

	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *SQLTokenizer) acceptRun(valid string) {
	for strings.ContainsRune(valid, l.nextRune()) {
	}
	l.backup()
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *SQLTokenizer) errorf(format string, args ...any) stateFn {
	l.token = Token{TokError, l.start, fmt.Sprintf(format, args...), l.startLine}
	l.start = 0
	l.Pos = 0
	l.Input = l.Input[:0]

	return nil
}

// lexSQL scans the elements inside action delimiters.
func lexSQL(l *SQLTokenizer) stateFn {
	switch r := l.nextRune(); {
	case r == eof:
		return nil
	case isSpace(r):
		return lexSpace
	case r == '\'':
		return lexString
	case r == '-' && l.peekRune() == '-':
		return lexLineComment
	case r == '/' && l.peekRune() == '*':
		return lexBlockComment
	case r == '-' || ('0' <= r && r <= '9'):
		l.backup()

		return lexNumber
	case isDelimiter(r):
		return l.emit(TokDelimiter)
	case isAlphaNumeric(r):
		return lexKeyworOrIdentifier
	default:
		return l.errorf("unrecognized character in action: %#U", r)
	}
}

// lexLineComment scans a line comment
func lexLineComment(l *SQLTokenizer) stateFn {
	// read second minus, first already scanned
	_ = l.nextRune()

	for {
		r := l.nextRune()
		if r == eof || r == '\n' {
			break
		}
	}

	return lexSQL(l)
}

// lexLineComment scans a block comment
func lexBlockComment(l *SQLTokenizer) stateFn {
	// read asterisk, lead slash already scanned
	_ = l.nextRune()

Loop:
	for {
		switch l.nextRune() {
		case '*':
			if l.peekRune() == '/' {
				_ = l.nextRune()

				break Loop
			}
		case eof:
			return l.errorf("unterminated comment")
		}
	}

	return lexSQL(l)
}

// lexSpace scans a run of space characters.
// We have not consumed the first space, which is known to be present.
// Take care if there is a trim-marked right delimiter, which starts with a space.
func lexSpace(l *SQLTokenizer) stateFn {
	for {
		r := l.nextRune()
		if r == eof || !isSpace(r) {
			l.backup()

			break
		}
	}

	l.start = l.Pos

	return lexSQL(l)
}

// lexQuote scans a quoted string.
func lexString(l *SQLTokenizer) stateFn {
Loop:
	for {
		switch l.nextRune() {
		case '\\':
			if r := l.nextRune(); r != eof && r != '\n' {
				break
			}

			fallthrough
		case eof, '\n':
			return l.errorf("unterminated quoted string")
		case '\'':
			break Loop
		}
	}

	return l.emit(TokString)
}

// lexNumber scans a number: decimal, octal, hex, float, or imaginary. This
// isn't a perfect number scanner - for instance it accepts "." and "0x0.2"
// and "089" - but when it's wrong the input is invalid and the parser (via
// strconv) will notice.
func lexNumber(l *SQLTokenizer) stateFn {
	if !l.scanNumber() {
		return l.errorf("bad number syntax: %q", l.Input[l.start:l.Pos])
	}

	return l.emit(TokNumber)
}

func (l *SQLTokenizer) scanNumber() bool {
	// Optional leading sign.
	l.accept("-")

	// Is it hex?
	digits := "0123456789_"

	if l.accept("0") {
		// Note: Leading 0 does not mean octal in floats.
		if l.accept("xX") {
			digits = "0123456789abcdefABCDEF_"
		} else if l.accept("oO") {
			digits = "01234567_"
		} else if l.accept("bB") {
			digits = "01_"
		}
	}

	l.acceptRun(digits)

	if l.accept(".") {
		l.acceptRun(digits)
	}

	if len(digits) == 10+1 && l.accept("eE") {
		l.accept("+-")
		l.acceptRun("0123456789_")
	}

	if len(digits) == 16+6+1 && l.accept("pP") {
		l.accept("+-")
		l.acceptRun("0123456789_")
	}

	if isAlphaNumeric(l.peekRune()) {
		l.nextRune()

		return false
	}

	return true
}

// lexVariable scans a field or variable: [.$]Alphanumeric.
// The . or $ has been scanned.
func lexKeyworOrIdentifier(l *SQLTokenizer) stateFn {
	var r rune

	for {
		r = l.nextRune()
		if !isAlphaNumeric(r) {
			l.backup()

			break
		}
	}

	return l.emitIdentifier()
}

// isSpace reports whether r is a space character.
func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\r' || r == '\n'
}

// isAlphaNumeric reports whether r is an alphabetic, digit, or underscore.
func isAlphaNumeric(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// isDelimiter reports whether r is an alphabetic is delimiter
func isDelimiter(r rune) bool {
	return strings.ContainsRune(",=.()+-*/%", r)
}
