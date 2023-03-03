package planner_test

import (
	"fmt"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/metadata"
	"github.com/unhandled-exception/sophiadb/internal/pkg/planner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
)

var _ planner.Plan = &planner.ProductPlan{}

type ProductPlanTestSuite struct {
	Suite
}

func TestProductPlanTestSuite(t *testing.T) {
	suite.Run(t, new(ProductPlanTestSuite))
}

func (ts *ProductPlanTestSuite) newSUT(dataCount int) (*planner.ProductPlan, *transaction.Transaction, func()) {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, t.TempDir())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	md, err := metadata.NewManager(true, trx)
	require.NoError(t, err)

	tablename1 := testDataTable + "1"
	tablename2 := testDataTable + "2"

	require.NoError(t, md.CreateTable(tablename1, ts.testLayout().Schema, trx))
	require.NoError(t, md.CreateTable(tablename2, ts.secondTestLayout().Schema, trx))

	if dataCount > 0 {
		ts1, werr := scan.NewTableScan(trx, tablename1, ts.testLayout())
		require.NoError(t, werr)

		for i := 0; i < dataCount; i++ {
			require.NoErrorf(t, ts1.Insert(), "write insert i == %d", i)

			require.NoErrorf(t, ts1.SetInt64("id", int64(i+1)), "write int64 i == %d", i)
			require.NoErrorf(t, ts1.SetInt8("age", int8(i%5)), "write int8 i == %d", i)
			require.NoErrorf(t, ts1.SetString("name", fmt.Sprintf("user %d", i)), "write string i == %d", i)
		}

		ts2, werr := scan.NewTableScan(trx, tablename2, ts.secondTestLayout())
		require.NoError(t, werr)

		for k := 0; k < dataCount; k++ {
			require.NoError(t, ts2.Insert())
			require.NoError(t, ts2.SetInt64("position", int64(k+1)))
			require.NoError(t, ts2.SetString("job", fmt.Sprintf("job %d", k)))
			require.NoError(t, ts2.SetInt8("room", int8(k%128)))
			require.NoError(t, ts2.SetVal("_invisible", scan.NewInt64Constant(int64(k/2))))
		}
	}

	tp1, err := planner.NewTablePlan(trx, tablename1, md)
	require.NoError(t, err)

	tp2, err := planner.NewTablePlan(trx, tablename2, md)
	require.NoError(t, err)

	sut, err := planner.NewProductPlan(tp1, tp2)
	require.NoError(t, err)

	return sut, trx, func() {
		fm.Close()
	}
}

func (ts *ProductPlanTestSuite) TestPlan() {
	t := ts.T()

	const blocks = 50
	cnt := int((defaultTestBlockSize / ts.testLayout().SlotSize) * blocks)

	sut, trx, clean := ts.newSUT(cnt)
	defer clean()

	schema := records.NewSchema()
	schema.AddAll(ts.testLayout().Schema)
	schema.AddAll(ts.secondTestLayout().Schema)

	assert.Equal(t, "join (scan table data1) to (scan table data2)", sut.String())
	assert.Equal(t, schema.String(), sut.Schema().String())
	assert.EqualValues(t, cnt*cnt, sut.Records())
	assert.Greater(t, int64(cnt*blocks*blocks), sut.BlocksAccessed())

	c, ok := sut.DistinctValues("age")
	assert.True(t, ok)
	assert.EqualValues(t, 5, c)

	c, ok = sut.DistinctValues("room")
	assert.True(t, ok)
	assert.EqualValues(t, 128, c)

	sc, err := sut.Open()
	require.NoError(t, err)

	i := 0

	assert.NoError(t, scan.ForEach(sc, func() (stop bool, err error) {
		i++

		_, err = sc.GetInt64("id")
		require.NoError(t, err)

		return false, nil
	}))

	assert.EqualValues(t, cnt*cnt, i)

	require.NoError(t, trx.Commit())
}

func (ts *ProductPlanTestSuite) TestFailedToOpenPlan() {
	t := ts.T()

	mc := minimock.NewController(t)
	p1 := planner.NewPlanMock(mc).SchemaMock.Return(ts.testLayout().Schema).
		OpenMock.Return(nil, scan.ErrScan)
	p2 := planner.NewPlanMock(mc).SchemaMock.Return(ts.secondTestLayout().Schema).
		OpenMock.Return(nil, nil)

	sut1, err := planner.NewProductPlan(p1, p2)
	require.NoError(t, err)

	_, err = sut1.Open()
	assert.ErrorIs(t, err, scan.ErrScan)

	sut2, err := planner.NewProductPlan(p2, p1)
	require.NoError(t, err)

	_, err = sut2.Open()
	assert.ErrorIs(t, err, scan.ErrScan)
}
