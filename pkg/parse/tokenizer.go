package parse

// Тjrtyfqpth по идеи лексера из доклада Роба Пайка — https://www.youtube.com/watch?v=HxaD_trXwRE&t=1567
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
	tokError tokenType = iota
	tokEOF
	tokKeyword
	tokIdentifier
	tokString
	tokNumber
	tokDelimiter
)

var reservedIdentifiers = map[string]tokenType{
	"select":  tokKeyword,
	"from":    tokKeyword,
	"where":   tokKeyword,
	"and":     tokKeyword,
	"insert":  tokKeyword,
	"into":    tokKeyword,
	"values":  tokKeyword,
	"delete":  tokKeyword,
	"update":  tokKeyword,
	"set":     tokKeyword,
	"create":  tokKeyword,
	"table":   tokKeyword,
	"varchar": tokKeyword,
	"int":     tokKeyword,
	"int64":   tokKeyword,
	"int8":    tokKeyword,
	"view":    tokKeyword,
	"as":      tokKeyword,
	"index":   tokKeyword,
	"on":      tokKeyword,
	"using":   tokKeyword,
}

type token struct {
	typ  tokenType
	pos  Pos
	val  string
	line int
}

func (i token) String() string {
	switch {
	case i.typ == tokEOF:
		return "{EOF}"
	case i.typ == tokError:
		return fmt.Sprintf("/%s/", i.val)
	case i.typ == tokKeyword:
		return fmt.Sprintf("<%s>", i.val)
	case i.typ == tokIdentifier:
		return fmt.Sprintf("[%s]", i.val)
	}

	return i.val
}

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*sqlTokenizer) stateFn

// sqlTokenizer holds the state of the scanner.
type sqlTokenizer struct {
	input     string // the string being scanned
	pos       Pos    // current position in the input
	start     Pos    // start position of this item
	atEOF     bool   // we have hit the end of input and returned eof
	line      int    // 1+number of newlines seen
	startLine int    // start line of this item
	token     token  // item to return to parser
}

func newSQLtokenizer(input string) *sqlTokenizer {
	l := &sqlTokenizer{
		input:     input,
		line:      1,
		startLine: 1,
	}

	return l
}

// nextToken returns the next item from the input.
// Called by the parser, not in the lexing goroutine.
func (l *sqlTokenizer) nextToken() {
	l.token = token{tokEOF, l.pos, "EOF", l.startLine}

	state := lexSQL

	for {
		state = state(l)
		if state == nil {
			return
		}
	}
}

// nextRune returns the nextRune rune in the input.
func (l *sqlTokenizer) nextRune() rune {
	if int(l.pos) >= len(l.input) {
		l.atEOF = true

		return eof
	}

	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += Pos(w)

	if r == '\n' {
		l.line++
	}

	return r
}

// backup steps back one rune.
func (l *sqlTokenizer) backup() {
	if !l.atEOF && l.pos > 0 {
		r, w := utf8.DecodeLastRuneInString(l.input[:l.pos])
		l.pos -= Pos(w)
		// Correct newline count.
		if r == '\n' {
			l.line--
		}
	}
}

func (l *sqlTokenizer) currentToken() token {
	return l.token
}

// thisToken returns the item at the current input point with the specified type
// and advances the input.
func (l *sqlTokenizer) thisToken(t tokenType) token {
	i := token{t, l.start, l.input[l.start:l.pos], l.startLine}
	l.start = l.pos
	l.startLine = l.line

	return i
}

// emit passes the trailing text as an item back to the parser.
func (l *sqlTokenizer) emit(t tokenType) stateFn {
	return l.emitToken(l.thisToken(t))
}

// emit passes the trailing text as an item back to the parser.
func (l *sqlTokenizer) emitIdentifier() stateFn {
	tok := l.thisToken(tokIdentifier)

	keyword := strings.ToLower(tok.val)
	if typ, ok := reservedIdentifiers[keyword]; ok {
		tok.typ = typ
		tok.val = keyword
	}

	return l.emitToken(tok)
}

// emitToken passes the specified item to the parser.
func (l *sqlTokenizer) emitToken(i token) stateFn {
	l.token = i

	return nil
}

// accept consumes the next rune if it's from the valid set.
func (l *sqlTokenizer) accept(valid string) bool {
	if strings.ContainsRune(valid, l.nextRune()) {
		return true
	}

	l.backup()

	return false
}

// acceptRun consumes a run of runes from the valid set.
func (l *sqlTokenizer) acceptRun(valid string) {
	for strings.ContainsRune(valid, l.nextRune()) {
	}
	l.backup()
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *sqlTokenizer) errorf(format string, args ...any) stateFn {
	l.token = token{tokError, l.start, fmt.Sprintf(format, args...), l.startLine}
	l.start = 0
	l.pos = 0
	l.input = l.input[:0]

	return nil
}

// lexSQL scans the elements inside action delimiters.
func lexSQL(l *sqlTokenizer) stateFn {
	switch r := l.nextRune(); {
	case r == eof:
		return nil
	case isSpace(r):
		return lexSpace
	case r == '\'':
		return lexString
	case r == '-' || ('0' <= r && r <= '9'):
		l.backup()

		return lexNumber
	case isDelimiter(r):
		return l.emit(tokDelimiter)
	case isAlphaNumeric(r):
		return lexKeyworOrIdentifier
	default:
		return l.errorf("unrecognized character in action: %#U", r)
	}
}

// lexSpace scans a run of space characters.
// We have not consumed the first space, which is known to be present.
// Take care if there is a trim-marked right delimiter, which starts with a space.
func lexSpace(l *sqlTokenizer) stateFn {
	for {
		r := l.nextRune()
		if r == eof || !isSpace(r) {
			l.backup()

			break
		}
	}

	l.start = l.pos

	return lexSQL(l)
}

// lexQuote scans a quoted string.
func lexString(l *sqlTokenizer) stateFn {
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

	return l.emit(tokString)
}

// lexNumber scans a number: decimal, octal, hex, float, or imaginary. This
// isn't a perfect number scanner - for instance it accepts "." and "0x0.2"
// and "089" - but when it's wrong the input is invalid and the parser (via
// strconv) will notice.
func lexNumber(l *sqlTokenizer) stateFn {
	if !l.scanNumber() {
		return l.errorf("bad number syntax: %q", l.input[l.start:l.pos])
	}

	return l.emit(tokNumber)
}

func (l *sqlTokenizer) scanNumber() bool {
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

	return true
}

// lexVariable scans a field or variable: [.$]Alphanumeric.
// The . or $ has been scanned.
func lexKeyworOrIdentifier(l *sqlTokenizer) stateFn {
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
