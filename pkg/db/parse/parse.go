// Токенайзер для поиска плейсхолдеров в запросах.
// Плейсхолдеры могут быть позиционные "?" и именованые ":name". Подстановки не сработают в строках в одинарных и двойных кавычках и в комментариях.
// Токенайзер лояльно относится к нарушению синтаксиса SQL — незакрытым кавычкам и комментариям. проверять семантику запросов будет парсер запросов Софии.

package parse

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

type tokenType int

const (
	TokError tokenType = iota
	TokEOF
	TokText
	TokNamed
	TokPositional
)

const (
	eof = -1
	sol = -2
)

// PlaceholderToken хранит описание токена
type PlaceholderToken struct {
	Typ  tokenType // тип токена
	Pos  int       // позиция токена в потоке
	Val  string    // значение токена
	Line int       // строка с ошибкой
}

// String возвращает строковое представление токена
func (t PlaceholderToken) String() string {
	switch t.Typ { //nolint:exhaustive
	case TokEOF:
		return "{EOF}"
	case TokError:
		return fmt.Sprintf("/%s/", t.Val)
	case TokNamed:
		return fmt.Sprintf("{%s}", t.Val)
	case TokPositional:
		return fmt.Sprintf("[%s]", t.Val)
	default:
		return t.Val
	}
}

// stateFn представляет текущее состояние автомата разбора потока токенов
type stateFn func(*PlaceholdersLexer) stateFn

// PlaceholdersLexer хранит состояние сканера потока токенов
type PlaceholdersLexer struct {
	input     string           // строка с потоком токенов
	token     PlaceholderToken // текуйщий разобраный токен в потоке
	line      int              // текущая строка в потоке
	pos       int              // текущая позиция сканера в потоке
	start     int              // начало текущего токена в потоке
	startLine int              // строка с началом текущего токена в потоке
	atEOF     bool             // признак, что мы достигли конца потока токенов
}

// NewPlaceholdersLexer создаёт новый сканер потока токенов
func NewPlaceholdersLexer(input string) *PlaceholdersLexer {
	l := &PlaceholdersLexer{
		input:     input,
		line:      1,
		startLine: 1,
	}

	return l
}

// Token возвращает текущий разобранный токен в потоке
func (l *PlaceholdersLexer) Token() PlaceholderToken {
	return l.token
}

// NextToken разбирает следующий токен в потоке
func (l *PlaceholdersLexer) NextToken() {
	l.token = PlaceholderToken{TokEOF, l.pos, "{EOF}", l.startLine}

	state := lexText

	for {
		state = state(l)
		if state == nil {
			return
		}
	}
}

// nextRune возвращает следующую руну из потока
func (l *PlaceholdersLexer) next() rune {
	if l.pos >= len(l.input) {
		l.atEOF = true

		return eof
	}

	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += w

	// Увеличиваем указатель текущей строки при переходе через конец строки
	if r == '\n' {
		l.line++
	}

	return r
}

// peek возвращает следующую руну не смещая указатель в потоке
func (l *PlaceholdersLexer) peek() rune {
	r := l.next()
	l.backup()

	return r
}

// backup сдвигает указатель в потоке рун на одну позицию назад
func (l *PlaceholdersLexer) backup() rune {
	if l.atEOF {
		return eof
	}

	if l.pos > 0 {
		r, w := utf8.DecodeLastRuneInString(l.input[:l.pos])
		l.pos -= w

		// Уменьшаем указатель текущей строки при возврате через конец строки
		if r == '\n' {
			l.line--
		}

		return r
	}

	return sol
}

// emit записывает в сканер последний токен
func (l *PlaceholdersLexer) emit(typ tokenType) stateFn {
	return l.emitToken(l.thisToken(typ))
}

// emitToken записывает токен в сканер
func (l *PlaceholdersLexer) emitToken(t PlaceholderToken) stateFn {
	l.token = t
	l.start = t.Pos
	l.startLine = l.line

	return nil
}

// thisToken создаёт токен с типом typ из текущего положения указателей в потоке
func (l *PlaceholdersLexer) thisToken(typ tokenType) PlaceholderToken {
	t := PlaceholderToken{
		Typ:  typ,
		Pos:  l.pos,
		Val:  l.input[l.start:l.pos],
		Line: l.startLine,
	}

	return t
}

// errorf записывает в сканер токен с ошибкой и возвращает nil,
// чтобы остановить сканирование потока в методе NextToken
func (l *PlaceholdersLexer) errorf(format string, args ...any) stateFn {
	l.token = PlaceholderToken{
		Typ:  TokError,
		Val:  fmt.Sprintf(format, args...),
		Pos:  l.start,
		Line: l.startLine,
	}

	l.start = 0
	l.pos = 0
	l.input = l.input[:0]

	return nil
}

// hasToken проверяет есть ли в потоке символы для токена
func (l *PlaceholdersLexer) hasToken() bool {
	return (l.atEOF && l.pos > 0 && l.pos > l.start) || (!l.atEOF && l.pos-1 > l.start)
}

// lexText — основной цикл сканера
func lexText(l *PlaceholdersLexer) stateFn {
	for {
		switch r := l.next(); {
		case r == eof:
			if l.hasToken() {
				return l.emit(TokText)
			}

			return nil
		case isQuote(r):
			return lexQuotedString
		case r == '-' && l.peek() == '-':
			return lexLineComment
		case r == '/' && l.peek() == '*':
			return lexBlockComment
		case r == '?':
			if l.hasToken() {
				l.backup()

				return l.emit(TokText)
			}

			return l.emit(TokPositional)
		case r == ':' && isNameStart(l.peek()):
			if l.hasToken() {
				l.backup()

				return l.emit(TokText)
			}

			return lexNamed
		}
	}
}

// lexNamed разбирает именованный параметр в запросе
func lexNamed(l *PlaceholdersLexer) stateFn {
Loop:
	for {
		switch r := l.next(); {
		case isAlphaNumeric(r):
			continue
		case r == eof:
			break Loop
		case r == '?':
			return l.errorf("a named parameter '%s' cannot end with a question mark", l.input[l.start:l.pos])
		default:
			l.backup()

			break Loop
		}
	}

	return l.emit(TokNamed)
}

// lexQuoteString пропускает в потоке строку в кавычках с учетом экранирования через \
func lexQuotedString(l *PlaceholdersLexer) stateFn {
	l.backup() // возвращаемся к предыдущей руне, чтобы определить тип кавычек

	quote := l.next()

Loop:
	for {
		switch l.next() {
		case '\\':
			if r := l.next(); r != eof && r != '\n' {
				continue
			}

			fallthrough
		case eof, '\n':
			break Loop
		case quote:
			break Loop
		}
	}

	return lexText
}

// lexLineComment пропускает в потоке однострочный комментарий
func lexLineComment(l *PlaceholdersLexer) stateFn {
	// read second minus, first already scanned
	_ = l.next()

	for {
		r := l.next()
		if r == eof || r == '\n' {
			break
		}
	}

	return lexText
}

// lexLineComment пропускает в потоке блочный комментарий
func lexBlockComment(l *PlaceholdersLexer) stateFn {
	// read asterisk, lead slash already scanned
	_ = l.next()

Loop:
	for {
		switch l.next() {
		case '*':
			if l.peek() == '/' {
				l.next()

				break Loop
			}
		case eof:
			break Loop
		}
	}

	return lexText
}

// isQuote проверяет является ли руна кавычкой
func isQuote(r rune) bool {
	return r == '\'' || r == '"'
}

// isAlphanumeric проверяет является ли символ допустимым в слове
func isAlphaNumeric(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// isNameStart проверяет явлеяется ли символ допустимым в начале слова
func isNameStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}
