package scan_test

import (
	"fmt"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
	"github.com/unhandled-exception/sophiadb/pkg/tx/transaction"
)

type ProductScanTestsuite struct {
	Suite
}

var _ scan.Scan = &scan.ProductScan{}

func TestProductScanTestsuite(t *testing.T) {
	suite.Run(t, new(ProductScanTestsuite))
}

func (ts *ProductScanTestsuite) secondTestLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddField("position", records.Int64Field, 20)
	schema.AddField("job", records.StringField, 20)
	schema.AddField("room", records.Int8Field, 0)
	schema.AddField("_invisible", records.Int64Field, 0)

	return records.NewLayout(schema)
}

func (ts *ProductScanTestsuite) TestSchema() {
	t := ts.T()

	schema1 := ts.testLayout().Schema
	schema2 := ts.secondTestLayout().Schema

	mc := minimock.NewController(t)
	s1 := scan.NewScanMock(mc).SchemaMock.Return(schema1)
	s2 := scan.NewScanMock(mc).SchemaMock.Return(schema2)

	sut := scan.NewProductScan(s1, s2)

	assert.Equal(t, "id int64, name varchar(25), age int8, _hidden int64, position int64, job varchar(20), room int8, _invisible int64", sut.Schema().String())
	assert.True(t, sut.HasField("room"))
}

func (ts *ProductScanTestsuite) TestEmptyLeftScan() {
	t := ts.T()

	schema1 := ts.testLayout().Schema
	schema2 := ts.secondTestLayout().Schema

	mc := minimock.NewController(t)
	s1 := scan.NewScanMock(mc).SchemaMock.Return(schema1).BeforeFirstMock.Return(nil).NextMock.Return(false, nil)
	s2 := scan.NewScanMock(mc).SchemaMock.Return(schema2)

	sut := scan.NewProductScan(s1, s2)

	err := sut.BeforeFirst()
	assert.ErrorIs(t, err, scan.ErrEmptyScan)
}

func (ts *ProductScanTestsuite) prepareTestTables(table1Name string, records1 int, table2Name string, records2 int) (*transaction.TRXManager, func()) {
	t := ts.T()

	layout1 := ts.testLayout()
	layout2 := ts.secondTestLayout()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, "")

	tx, err := trxMan.Transaction()
	require.NoError(t, err)

	ts1, err := scan.NewTableScan(tx, table1Name, layout1)
	require.NoError(t, err)

	ts2, err := scan.NewTableScan(tx, table2Name, layout2)
	require.NoError(t, err)

	for i := 0; i < records1; i++ {
		require.NoError(t, ts1.Insert())
		require.NoError(t, ts1.SetInt64("id", int64(i)))
		require.NoError(t, ts1.SetString("name", fmt.Sprintf("user %d", i)))
		require.NoError(t, ts1.SetInt8("age", int8(i%128)))
		require.NoError(t, ts1.SetInt64("_hidden", int64(i/2)))
	}

	for k := 0; k < records2; k++ {
		require.NoError(t, ts2.Insert())
		require.NoError(t, ts2.SetInt64("position", int64(k)))
		require.NoError(t, ts2.SetString("job", fmt.Sprintf("job %d", k)))
		require.NoError(t, ts2.SetInt8("room", int8(k%128)))
		require.NoError(t, ts2.SetVal("_invisible", scan.NewInt64Constant(int64(k/2))))
	}

	ts1.Close()
	ts2.Close()

	require.NoError(t, tx.Commit())

	return trxMan, func() {
		require.NoError(t, fm.Close())
	}
}

func (ts *ProductScanTestsuite) TestIterate() {
	t := ts.T()

	layout1 := ts.testLayout()
	layout2 := ts.secondTestLayout()

	table1Name := "table1"
	records1 := 500

	table2Name := "table2"
	records2 := 100

	trxMan, final := ts.prepareTestTables(table1Name, records1, table2Name, records2)
	defer final()

	tx, err := trxMan.Transaction()
	require.NoError(t, err)

	ts1, err := scan.NewTableScan(tx, table1Name, layout1)
	require.NoError(t, err)

	ts2, err := scan.NewTableScan(tx, table2Name, layout2)
	require.NoError(t, err)

	sut := scan.NewProductScan(ts1, ts2)

	require.NoError(t, sut.BeforeFirst())

	for i := 0; i < records1; i++ {
		for k := 0; k < records2; k++ {
			ok, err := sut.Next()
			require.True(t, ok)
			require.NoError(t, err)

			id, err := sut.GetInt64("id")
			require.NoError(t, err)
			require.EqualValues(t, i, id)

			name, err := sut.GetString("name")
			require.NoError(t, err)
			require.EqualValues(t, fmt.Sprintf("user %d", i), name)

			age, err := sut.GetInt8("age")
			require.NoError(t, err)
			require.EqualValues(t, i%128, age)

			hidden, err := sut.GetVal("_hidden")
			require.NoError(t, err)
			require.Equal(t, scan.CompEqual, scan.NewInt64Constant(int64(i/2)).CompareTo(hidden))

			position, err := sut.GetInt64("position")
			require.NoError(t, err)
			require.EqualValues(t, k, position)

			job, err := sut.GetString("job")
			require.NoError(t, err)
			require.EqualValues(t, fmt.Sprintf("job %d", k), job)

			room, err := sut.GetInt8("room")
			require.NoError(t, err)
			require.EqualValues(t, k%128, room)

			invisible, err := sut.GetVal("_invisible")
			require.NoError(t, err)
			require.Equal(t, scan.CompEqual, scan.NewInt64Constant(int64(k/2)).CompareTo(invisible))
		}
	}

	sut.Close()

	require.NoError(t, tx.Rollback())
}
