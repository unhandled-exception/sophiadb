package db_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

type table1Record struct {
	ID   int64
	Name string
	Age  int8
}

func scanRowToRecord(t *testing.T, row *sql.Row) table1Record {
	require.NoError(t, row.Err())

	var rec table1Record

	err := row.Scan(&rec.ID, &rec.Name, &rec.Age)
	require.NoError(t, err)

	return rec
}
