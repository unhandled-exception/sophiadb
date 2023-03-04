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

// Token описывает токен из потока токенов
type Token struct {
	Typ  tokenType
	Pos  Pos
	Val  string
	Line int
}

// String форматирует токен в строку
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

// stateFn представляет текущее состояние сканера в виде функци, возвращающей следующее состояние
type stateFn func(*SQLTokenizer) stateFn

// SQLTokenizer содержит состояние сканера
type SQLTokenizer struct {
	Input string // строка с потоком токенов
	Pos   Pos    // позиция сканера в потоке
	Line  int    // текущая строка в потоке

	token     Token // последний разобранный токен в потоке
	start     Pos   // начало текущего токена в потоке
	startLine int   // строка с началом текущего токена
	atEOF     bool  // признак, что мы достигли конца потока токенов
}

// NewSQLTokenizer создаёт новый сканер
func NewSQLtokenizer(input string) *SQLTokenizer {
	l := &SQLTokenizer{
		Input:     input,
		Line:      1,
		startLine: 1,
	}

	return l
}

// CurrentToken последний разобранный токен
func (l *SQLTokenizer) CurrentToken() Token {
	return l.token
}

// NextToken разбирает следующий токен в потоке
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

// nextRune возвращает следующую руну из потока
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

// peek возвращает следующую руну не смещая указатель в потоке
func (l *SQLTokenizer) peekRune() rune {
	r := l.nextRune()
	l.backup()

	return r
}

// backup сдвигает указатель в потоке рун на одну позицию назад
func (l *SQLTokenizer) backup() {
	if !l.atEOF && l.Pos > 0 {
		r, w := utf8.DecodeLastRuneInString(l.Input[:l.Pos])
		l.Pos -= Pos(w)

		// Корректируем положение строки в потоки
		if r == '\n' {
			l.Line--
		}
	}
}

// thisToken создаёт токен, на который указывают start- и pos-указатели в потоке
func (l *SQLTokenizer) thisToken(t tokenType) Token {
	i := Token{t, l.start, l.Input[l.start:l.Pos], l.startLine}
	l.start = l.Pos
	l.startLine = l.Line

	return i
}

// emit записывает в сканер последний токен
func (l *SQLTokenizer) emit(t tokenType) stateFn {
	return l.emitToken(l.thisToken(t))
}

// emitToken записывает идентификатор в сканер
func (l *SQLTokenizer) emitIdentifier() stateFn {
	tok := l.thisToken(TokIdentifier)

	keyword := strings.ToLower(tok.Val)
	if typ, ok := reservedIdentifiers[keyword]; ok {
		tok.Typ = typ
		tok.Val = keyword
	}

	return l.emitToken(tok)
}

// emitToken записывает токен в сканер
func (l *SQLTokenizer) emitToken(i Token) stateFn {
	l.token = i

	return nil
}

// accept сдвигает позицию если в потоке есть символ из строки valid
func (l *SQLTokenizer) accept(valid string) bool {
	if strings.ContainsRune(valid, l.nextRune()) {
		return true
	}

	l.backup()

	return false
}

// acceptRun сдвигает позицию пока в потоке есть символы из строки valid
func (l *SQLTokenizer) acceptRun(valid string) {
	for strings.ContainsRune(valid, l.nextRune()) {
	}
	l.backup()
}

// errorf записывает в сканер токен с ошибкой и возвращает nil,
// чтобы остановить сканирование потока в методе NextToken
func (l *SQLTokenizer) errorf(format string, args ...any) stateFn {
	l.token = Token{TokError, l.start, fmt.Sprintf(format, args...), l.startLine}
	l.start = 0
	l.Pos = 0
	l.Input = l.Input[:0]

	return nil
}

// lexSQL основной цикл сканера
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

// lexLineComment разбирает однострочный комментарий
func lexLineComment(l *SQLTokenizer) stateFn {
	// читаем второй минус, первый уже в сканере
	_ = l.nextRune()

	for {
		r := l.nextRune()
		if r == eof || r == '\n' {
			break
		}
	}

	return lexSQL
}

// lexLineComment разбирает многострочный комментарий
func lexBlockComment(l *SQLTokenizer) stateFn {
	// читаем звёздочку, ведущий слеш уже в сканере
	_ = l.nextRune()

Loop:
	for {
		switch l.nextRune() {
		case '*':
			if l.peekRune() == '/' {
				l.nextRune()
				l.start = l.Pos

				break Loop
			}
		case eof:
			return l.errorf("unterminated comment")
		}
	}

	return lexSQL
}

// lexSpace разбирает пробельные символы (разделители). Первый символ уже в сканере
func lexSpace(l *SQLTokenizer) stateFn {
	for {
		r := l.nextRune()
		if r == eof || !isSpace(r) {
			l.backup()

			break
		}
	}

	l.start = l.Pos

	return lexSQL
}

// lexQuote разбирает строку в одинарных кавычках
func lexString(l *SQLTokenizer) stateFn {
Loop:
	for {
		switch l.nextRune() {
		case '\\':
			if r := l.nextRune(); r != eof && r != '\n' {
				continue
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

// lexNumber разбирает числа
func lexNumber(l *SQLTokenizer) stateFn {
	if !l.scanNumber() {
		return l.errorf("bad number syntax: %q", l.Input[l.start:l.Pos])
	}

	return l.emit(TokNumber)
}

// scanNumber сканирует число для lexNumber
func (l *SQLTokenizer) scanNumber() bool {
	// опциональный минус в начале числа
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

// lexKeyworOrIdentifier разбирает ключевые слова и идентификаторы
func lexKeyworOrIdentifier(l *SQLTokenizer) stateFn {
	var r rune

Loop:
	for {
		r = l.nextRune()
		switch {
		case isQuote(r):
			return l.errorf("bad syntax: %q", l.Input[l.start:l.Pos])
		case !isAlphaNumeric(r):
			l.backup()

			break Loop
		}
	}

	return l.emitIdentifier()
}

// isQuote проверяет является ли руна кавычкой
func isQuote(r rune) bool {
	return r == '\'' || r == '"'
}

// isSpace проверяет, что руна пробелный символ
func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\r' || r == '\n'
}

// isAlphanumeric проверяет является ли символ допустимым в слове
func isAlphaNumeric(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// isDelimiter проверяет, что руна символ-разделитель
func isDelimiter(r rune) bool {
	return strings.ContainsRune(",=.()+-*/%", r)
}
