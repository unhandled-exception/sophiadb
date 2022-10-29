package parse //nolint:testpackage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLexer(t *testing.T) {
	s := `select one, two, three from table1, table2, where id = 1 and name = 'name \'1\''`

	sut := newSQLtokenizer(s)

	tokens := []string{}

	var err error

loop:
	for {
		sut.nextToken()

		tok := sut.currentToken()

		if tok.typ == tokEOF {
			break loop
		}

		if tok.typ == tokError {
			err = fmt.Errorf("parse fail: %s", tok.val)

			break loop
		}

		tokens = append(tokens, tok.String())
	}

	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{
			"<select>",
			"[one]", ",",
			"[two]", ",",
			"[three]",
			"<from>", "[table1]", ",", "[table2]", ",",
			"<where>",
			"[id]", "=", "1",
			"<and>", "[name]", "=", "'name \\'1\\''",
		},
		tokens,
	)
}
