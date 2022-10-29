package db

import (
	"database/sql/driver"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var placeholdersRe = regexp.MustCompile(`(?:\?|\:\w+)`)

func applyPlaceholders(query string, args []driver.NamedValue) (string, error) {
	positional := fetchPositionalArgs(args)
	named := fetchNamedArgs(args)

	curPos := -1
	maxPositional := len(positional)

	var err error

	applied := placeholdersRe.ReplaceAllFunc([]byte(query), func(b []byte) []byte {
		if err != nil {
			return b
		}

		var res []byte = b

		switch b[0] {
		case '?':
			if curPos++; curPos < maxPositional {
				res, err = serializeValue(positional[curPos])
			} else {
				err = errors.WithMessage(ErrFailedProcessPlaceholders, "not enough values for placeholders")
			}
		case ':':
			name := string(b[1:])
			if v, ok := named[name]; ok {
				res, err = serializeValue(v)
			} else {
				err = errors.WithMessagef(ErrFailedProcessPlaceholders, "unknonw key '%s'", name)
			}
		}

		return res
	})

	if err != nil {
		return "", err
	}

	return string(applied), nil
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
		return []byte(strconv.FormatInt(v, 10)), nil //nolint:gomnd
	case string:
		return []byte("'" + strings.ReplaceAll(v, "'", `\'`) + "'"), nil
	}

	return []byte{}, errors.WithMessagef(ErrUnserializableValue, "%v", value)
}
