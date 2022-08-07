package metadata_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
)

type TablesTestSuite struct {
	Suite
}

func TestTablesTestSuite(t *testing.T) {
	suite.Run(t, new(TablesTestSuite))
}

func (ts *TablesTestSuite) TestCreateAndFetchTable() {
	t := ts.T()

	trxMan, clean := ts.newTRXManager(defaultLockTimeout, "")
	defer clean()

	trx1, err := trxMan.Transaction()
	require.NoError(t, err)

	sut1, err := metadata.NewTables(true, trx1)
	require.NoError(t, err)
	assert.NotNil(t, sut1)

	const testTable = "test_table"

	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	err = sut1.CreateTable(testTable, schema, trx1)
	require.NoError(t, err)

	require.NoError(t, trx1.Commit())

	trx2, err := trxMan.Transaction()
	require.NoError(t, err)

	sut2, err := metadata.NewTables(false, trx2)
	require.NoError(t, err)

	layout, err := sut2.Layout(testTable, trx2)
	require.NoError(t, err)

	assert.Equal(t, "[id: int64], [name: string(25)], [age: int8]", layout.Schema.String())

	require.NoError(t, trx2.Commit())
}

func (ts *TablesTestSuite) TestTableNotFound() {
	t := ts.T()

	trxMan, clean := ts.newTRXManager(defaultLockTimeout, "")
	defer clean()

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	sut, err := metadata.NewTables(true, trx)
	require.NoError(t, err)
	assert.NotNil(t, sut)

	testTable := "test_table"

	_, err = sut.Layout(testTable, trx)
	require.ErrorIs(t, err, metadata.ErrTableNotFound)
}

func (ts *TablesTestSuite) TestSchemaNotFound() {
	t := ts.T()

	trxMan, clean := ts.newTRXManager(defaultLockTimeout, "")
	defer clean()

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	sut, err := metadata.NewTables(true, trx)
	require.NoError(t, err)
	assert.NotNil(t, sut)

	testTable := "test_table"

	schema := records.NewSchema()

	err = sut.CreateTable(testTable, schema, trx)
	require.NoError(t, err)

	_, err = sut.Layout(testTable, trx)
	require.ErrorIs(t, err, metadata.ErrTableSchemaNotFound)
}
