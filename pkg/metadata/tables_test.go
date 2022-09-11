package metadata_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
	"github.com/unhandled-exception/sophiadb/pkg/tx/transaction"
)

type TablesTestSuite struct {
	Suite
}

func TestTablesTestSuite(t *testing.T) {
	suite.Run(t, new(TablesTestSuite))
}

func (ts *TablesTestSuite) newSUT(t *testing.T, path string) (*metadata.Tables, *transaction.Transaction, func()) {
	if path == "" {
		path = t.TempDir()
	}

	trxMan, clean := ts.newTRXManager(defaultLockTimeout, path)
	defer clean()

	strx, err := trxMan.Transaction()
	require.NoError(t, err)

	sut, err := metadata.NewTables(true, strx)
	require.NoError(t, err)

	require.NoError(t, strx.Commit())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	return sut, trx, func() {
		clean()
	}
}

func (ts *TablesTestSuite) TestCreateTable_Ok() {
	t := ts.T()

	path := t.TempDir()

	sut, trx, clean := ts.newSUT(t, path)
	defer clean()

	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	layout := records.NewLayout(schema)

	for i := 0; i < 100; i++ {
		err := sut.CreateTable(fmt.Sprintf("test_table_%d", i), schema, trx)
		require.NoError(t, err)
	}

	require.NoError(t, trx.Commit())

	recs, err := scan.NewTableScan(trx, sut.TcatTable, sut.TcatLayout)
	require.NoError(t, err)

	i := 0

	require.NoError(t, scan.ForEach(recs, func() (bool, error) {
		tn, ierr := recs.GetString(metadata.TcatTableNameField)
		require.NoError(t, ierr)

		// Пропускаем две служебные таблички
		if tn == sut.TcatTable || tn == sut.FcatTable {
			return false, nil
		}

		tss, ierr := recs.GetInt64(metadata.TcatSlotsizeField)
		assert.NoError(t, ierr)

		assert.Equal(t, fmt.Sprintf("test_table_%d", i), tn)
		assert.EqualValues(t, layout.SlotSize, tss)

		i++

		return false, nil
	}))

	assert.Equal(t, 100, i)

	recs, err = scan.NewTableScan(trx, sut.FcatTable, sut.FcatLayout)
	require.NoError(t, err)

	i = -1
	lt := ""

	require.NoError(t, scan.ForEach(recs, func() (bool, error) {
		type fieldInfo struct {
			TableName string
			FieldName string
			FieldType int8
			Length    int64
			Offset    int64
		}

		fi := fieldInfo{}

		tn, err := recs.GetString(metadata.TcatTableNameField)
		require.NoError(t, err)

		// Пропускаем две служебные таблички
		if tn == sut.TcatTable || tn == sut.FcatTable {
			return false, nil
		}

		if lt != tn {
			i++
			lt = tn
		}

		require.NoError(t, scan.ForEachValue(recs, func(name string, fieldType records.FieldType, value interface{}) (bool, error) {
			var ok bool

			switch name {
			case metadata.FcatTableNameField:
				fi.TableName, ok = value.(string)
			case metadata.FcatFieldNameField:
				fi.FieldName, ok = value.(string)
			case metadata.FcatTypeField:
				fi.FieldType, ok = value.(int8)
			case metadata.FcatLengthField:
				fi.Length, ok = value.(int64)
			case metadata.FcatOffsetField:
				fi.Offset, ok = value.(int64)
			}

			if !ok {
				return true, fmt.Errorf("failed to covert value for %s[%d]", name, fieldType)
			}

			return false, nil
		}))

		switch fi.FieldName {
		case "id":
			assert.Equal(t, fieldInfo{TableName: fmt.Sprintf("test_table_%d", i), FieldName: "id", FieldType: 1, Length: 0, Offset: 1}, fi)
		case "name":
			assert.Equal(t, fieldInfo{TableName: fmt.Sprintf("test_table_%d", i), FieldName: "name", FieldType: 2, Length: 25, Offset: 9}, fi)
		case "age":
			assert.Equal(t, fieldInfo{TableName: fmt.Sprintf("test_table_%d", i), FieldName: "age", FieldType: 3, Length: 0, Offset: 113}, fi)
		default:
			return true, fmt.Errorf("unknown field %s", fi.FieldName)
		}

		return false, nil
	}))

	assert.Equal(t, 99, i)
}

func (ts *TablesTestSuite) TestCreateTable_TableExists() {
	t := ts.T()

	sut, trx, clean := ts.newSUT(t, "")
	defer clean()

	const testTable = "test_table"

	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	err := sut.CreateTable(testTable, schema, trx)
	require.NoError(t, err)

	err = sut.CreateTable(testTable, schema, trx)
	require.ErrorIs(t, err, metadata.ErrTableExists)

	require.NoError(t, trx.Commit())
}

func (ts *TablesTestSuite) TestTableExists_Ok() {
	t := ts.T()

	sut, trx, clean := ts.newSUT(t, "")
	defer clean()

	const testTable = "test_table"

	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	err := sut.CreateTable(testTable, schema, trx)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		err = sut.CreateTable(fmt.Sprintf("%s_%d", testTable, i), schema, trx)
		require.NoError(t, err)
	}

	exists, err := sut.TableExists(testTable, trx)
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = sut.TableExists("unexistant", trx)
	require.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, trx.Commit())
}

func (ts *TablesTestSuite) TestLayout_Ok() {
	t := ts.T()

	path := t.TempDir()

	sut, trx, clean := ts.newSUT(t, path)
	defer clean()

	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	for i := 0; i < 100; i++ {
		err := sut.CreateTable(fmt.Sprintf("test_table_%d", i), schema, trx)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		layout, err := sut.Layout(fmt.Sprintf("test_table_%d", i), trx)
		require.NoError(t, err)

		assert.Equal(t, "[id: int64], [name: string(25)], [age: int8]", layout.Schema.String())
		assert.EqualValues(t, 114, layout.SlotSize)
	}
}

func (ts *TablesTestSuite) TestLayout_TableNotFound() {
	t := ts.T()

	sut, trx, clean := ts.newSUT(t, "")
	defer clean()

	testTable := "test_table"

	_, err := sut.Layout(testTable, trx)
	require.ErrorIs(t, err, metadata.ErrTableNotFound)
}

func (ts *TablesTestSuite) TestLayout_TestSchemaNotFound() {
	t := ts.T()

	sut, trx, clean := ts.newSUT(t, "")
	defer clean()

	testTable := "test_table"

	schema := records.NewSchema()

	err := sut.CreateTable(testTable, schema, trx)
	require.NoError(t, err)

	_, err = sut.Layout(testTable, trx)
	require.ErrorIs(t, err, metadata.ErrTableSchemaNotFound)
}

func (ts *TablesTestSuite) TestForEachTables() {
	t := ts.T()

	const testTable = "test_table"

	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	sut, trx, clean := ts.newSUT(t, "")
	defer clean()

	err := sut.CreateTable(testTable, schema, trx)
	require.NoError(t, err)

	tables := []string{}

	err = sut.ForEachTables(trx, func(tableName string) (bool, error) {
		tables = append(tables, tableName)

		return false, nil
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"sdb_tables", "sbb_tables_fields", "test_table"}, tables)
}
