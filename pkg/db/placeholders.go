package db

import (
	"database/sql/driver"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/pkg/db/parse"
)

func applyPlaceholders(query string, args []driver.NamedValue) (string, error) {
	positional := fetchPositionalArgs(args)
	named := fetchNamedArgs(args)

	curPos := -1
	maxPositional := len(positional)

	lexer := parse.NewPlaceholdersLexer(query)

	res := []string{}

	var err error

Loop:
	for {
		var val []byte

		lexer.NextToken()

		switch tok := lexer.Token(); tok.Typ {
		case parse.TokPositional:

			if curPos++; curPos < maxPositional {
				val, err = serializeValue(positional[curPos])
			} else {
				err = errors.WithMessage(ErrFailedProcessPlaceholders, "not enough values for placeholders")
			}

		case parse.TokNamed:
			name := tok.Val[1:]

			if v, ok := named[name]; ok {
				val, err = serializeValue(v)
			} else {
				err = errors.WithMessagef(ErrFailedProcessPlaceholders, "unknonw key '%s'", name)
			}

		case parse.TokText:
			res = append(res, tok.Val)
		case parse.TokError:
			err = errors.WithMessagef(ErrFailedProcessPlaceholders, lexer.Token().Val)

			fallthrough
		case parse.TokEOF:
			break Loop
		}

		if err != nil {
			break Loop
		}

		res = append(res, string(val))
	}

	if err != nil {
		return "", err
	}

	return strings.Join(res, ""), nil
}

func fetchNamedArgs(args []driver.NamedValue) map[string]driver.Value {
	named := make(map[string]driver.Value, len(args))

	for _, arg := range args {
		if arg.Name == "" {
			continue
		}

		named[arg.Name] = arg.Value
	}

	return named
}

func fetchPositionalArgs(args []driver.NamedValue) []driver.Value {
	positional := make([]driver.Value, 0, len(args))

	for _, arg := range args {
		if arg.Name != "" {
			continue
		}

		positional = append(positional, arg.Value)
	}

	return positional
}

func serializeValue(value driver.Value) ([]byte, error) {
	switch v := value.(type) {
	case int64:
		return []byte(strconv.FormatInt(v, 10)), nil //nolint:mnd
	case string:
		return []byte("'" + strings.ReplaceAll(v, "'", `\'`) + "'"), nil
	}

	return []byte{}, errors.WithMessagef(ErrUnserializableValue, "%v", value)
}
