package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/unhandled-exception/sophiadb/pkg/parse"
)

func TestSQLTokenizer(t *testing.T) {
	tests := []struct {
		query  string
		result []string
	}{
		{
			`select one, two, three from table1, table2, where id = 1 and name = 'name \'1\''`,
			[]string{"<select>", "[one]", ",", "[two]", ",", "[three]", "<from>", "[table1]", ",", "[table2]", ",", "<where>", "[id]", "=", "1", "<and>", "[name]", "=", "'name \\'1\\''", "{EOF}"},
		},
		{
			`create table table1 (id int64 ,name varchar ( 100), age int8 )`,
			[]string{"<create>", "<table>", "[table1]", "(", "[id]", "<int64>", ",", "[name]", "<varchar>", "(", "100", ")", ",", "[age]", "<int8>", ")", "{EOF}"},
		},
		{
			`123.34 0x12AF 0o777 0b01010101 23E+344`,
			[]string{"123.34", "0x12AF", "0o777", "0b01010101", "23E+344", "{EOF}"},
		},
		{
			`12355from table1`,
			[]string{"/bad number syntax: \"12355f\"/"},
		},
		{
			``,
			[]string{"{EOF}"},
		},
		{
			"       \t\t\t\n\n\n\n",
			[]string{"{EOF}"},
		},
		{
			`create table table1 {id int8}`,
			[]string{"<create>", "<table>", "[table1]", "/unrecognized character in action: U+007B '{'/"},
		},
		{
			`from 'name 1 to table2`,
			[]string{"<from>", "/unterminated quoted string/"},
		},
		{
			`from "name 1"`,
			[]string{"<from>", "/unrecognized character in action: U+0022 '\"'/"},
		},
	}

	for _, tc := range tests {
		tokens := tokenize(tc.query)
		assert.Equal(t, tc.result, tokens)
	}
}

func tokenize(s string) []string {
	sut := parse.NewSQLtokenizer(s)

	tokens := []string{}

loop:
	for {
		sut.NextToken()

		tok := sut.CurrentToken()

		if tok.Typ == parse.TokEOF || tok.Typ == parse.TokError {
			tokens = append(tokens, tok.String())

			break loop
		}

		tokens = append(tokens, tok.String())
	}

	return tokens
}
