package parse_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/unhandled-exception/sophiadb/pkg/db/parse"
)

func TestPlaceholdersLexer(t *testing.T) {
	tests := []struct {
		query  string
		result []string
	}{
		{
			`select *
			   from t1
			  where id = :id
			        and name := :name
					and age = :age : 2
					and ? and :param and ? and ?
			`,
			[]string{
				"select *\n\t\t\t   from t1\n\t\t\t  where id = ",
				"{:id}",
				"\n\t\t\t        and name := ",
				"{:name}",
				"\n\t\t\t\t\tand age = ",
				"{:age}",
				" : 2\n\t\t\t\t\tand ",
				"[?]",
				" and ",
				"{:param}",
				" and ",
				"[?]",
				" and ",
				"[?]",
				"\n\t\t\t",
				"{EOF}",
			},
		},
		{
			`-- Query :id
			select id, name, age
			from table1
			-- join table2 ousing (id) ? ? ?
			:test
			where
				1=?
				and name = :name -- end line :name
			-- end ?`,
			[]string{
				"-- Query :id\n\t\t\tselect id, name, age\n\t\t\tfrom table1\n\t\t\t-- join table2 ousing (id) ? ? ?\n\t\t\t",
				"{:test}",
				"\n\t\t\twhere\n\t\t\t\t1=",
				"[?]",
				"\n\t\t\t\tand name = ",
				"{:name}",
				" -- end line :name\n\t\t\t-- end ?",
				"{EOF}",
			},
		},
		{
			`
			/**** Query :id ****/	select id, name, age
			from table1
			/* join table 2 ?
			   -- using (:id)
			*/
			where
				1 = ?
				and name = :name
			/* end*/`,
			[]string{
				"\n\t\t\t/**** Query :id ****/\tselect id, name, age\n\t\t\tfrom table1\n\t\t\t/* join table 2 ?\n\t\t\t   -- using (:id)\n\t\t\t*/\n\t\t\twhere\n\t\t\t\t1 = ",
				"[?]",
				"\n\t\t\t\tand name = ",
				"{:name}",
				"\n\t\t\t/* end*/",
				"{EOF}",
			},
		},
		{
			`select * from /* :name ?`,
			[]string{"select * from /* :name ?", "{EOF}"},
		},
		{
			`select ':name from t`,
			[]string{`select ':name from t`, "{EOF}"},
		},
		{
			`select :name`,
			[]string{"select ", "{:name}", "{EOF}"},
		},
		{
			`select ?`,
			[]string{"select ", "[?]", "{EOF}"},
		},
		{
			`:name`,
			[]string{"{:name}", "{EOF}"},
		},
		{
			`?`,
			[]string{"[?]", "{EOF}"},
		},
		{
			`select :name?`,
			[]string{"select ", "/a named parameter ':name?' cannot end with a question mark/"},
		},
		{
			`select 'name \'?\''`,
			[]string{"select 'name \\'?\\''", "{EOF}"},
		},
		{
			`select 'test\`,
			[]string{"select 'test\\", "{EOF}"},
		},
		{
			"select 'test\\\n",
			[]string{"select 'test\\\n", "{EOF}"},
		},
	}

	for _, tc := range tests {
		tokens := tokenize(tc.query)
		assert.Equal(t, tc.result, tokens)
	}
}

func tokenize(s string) []string {
	sut := parse.NewPlaceholdersLexer(s)

	tokens := []string{}

loop:
	for {
		sut.NextToken()

		tok := sut.Token()

		if tok.Typ == parse.TokEOF || tok.Typ == parse.TokError {
			tokens = append(tokens, tok.String())

			break loop
		}

		tokens = append(tokens, tok.String())
	}

	return tokens
}
