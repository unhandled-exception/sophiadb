package scan_test

import (
	"fmt"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

var (
	_ scan.Scan       = &scan.SelectScan{}
	_ scan.UpdateScan = &scan.SelectScan{}
)

type SelectScanTestSuite struct {
	Suite
}

func TestSelectScanTestsuite(t *testing.T) {
	suite.Run(t, new(SelectScanTestSuite))
}

func (ts *SelectScanTestSuite) TestIterate() {
	t := ts.T()

	records := 1000

	var testID int64 = 777

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, "")
	defer fm.Close()

	trx1, err := trxMan.Transaction()
	require.NoError(t, err)

	ts1, err := scan.NewTableScan(trx1, testDataTable, ts.testLayout())
	require.NoError(t, err)

	for i := 0; i < records; i++ {
		require.NoError(t, ts1.Insert())

		require.NoError(t, ts1.SetInt64("id", int64(i)))
		require.NoError(t, ts1.SetString("name", fmt.Sprintf("user %d", i)))
		require.NoError(t, ts1.SetInt8("age", int8(i%128)))
		require.NoError(t, ts1.SetInt64("_hidden", int64(i/2)))
	}

	require.NoError(t, trx1.Commit())

	trx2, err := trxMan.Transaction()
	require.NoError(t, err)

	ts2, err := scan.NewTableScan(trx2, testDataTable, ts.testLayout())
	require.NoError(t, err)

	sut := scan.NewSelectScan(
		ts2,
		scan.NewAndPredicate(
			scan.NewEqualTerm(
				scan.NewFieldExpression("id"),
				scan.NewScalarExpression(scan.NewInt64Constant(testID)),
			),
		),
	)

	assert.True(t, sut.HasField("id"))

	defer sut.Close()

	i := 0

	require.NoError(t, scan.ForEach(sut, func() (bool, error) {
		i++

		id, err := sut.GetInt64("id")
		require.NoError(t, err)
		assert.EqualValues(t, testID, id)

		age, err := sut.GetInt8("age")
		require.NoError(t, err)
		assert.EqualValues(t, testID%128, age)

		name, err := sut.GetString("name")
		require.NoError(t, err)
		assert.EqualValues(t, fmt.Sprintf("user %d", testID), name)

		hidden, err := sut.GetVal("_hidden")
		require.NoError(t, err)
		assert.Equal(t, scan.CompEqual, scan.NewInt64Constant(testID/2).CompareTo(hidden))

		return false, nil
	}))

	assert.EqualValues(t, 1, i)

	require.NoError(t, trx2.Commit())
}

func (ts *SelectScanTestSuite) TestUpdate() {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, "")
	defer fm.Close()

	trx1, err := trxMan.Transaction()
	require.NoError(t, err)

	ts2, err := scan.NewTableScan(trx1, testDataTable, ts.testLayout())
	require.NoError(t, err)

	sut := scan.NewSelectScan(
		ts2,
		scan.NewAndPredicate(
			scan.NewEqualTerm(
				scan.NewFieldExpression("age"),
				scan.NewScalarExpression(scan.NewInt8Constant(2)),
			),
		),
	)

	defer sut.Close()

	for i := 0; i < 4; i++ {
		require.NoError(t, sut.Insert())

		require.NoError(t, sut.SetInt64("id", int64(i)))
		require.NoError(t, sut.SetString("name", fmt.Sprintf("user %d", i)))
		require.NoError(t, sut.SetInt8("age", 2))
		require.NoError(t, sut.SetVal("_hidden", scan.NewInt64Constant(int64(i/2))))
	}

	assert.Equal(t, types.RID{BlockNumber: 0, Slot: 3}, sut.RID())

	assert.NoError(t, sut.MoveToRID(types.RID{BlockNumber: 0, Slot: 0}))

	require.NoError(t, sut.Delete())

	cnt := 0

	require.NoError(t, scan.ForEach(sut, func() (bool, error) {
		cnt++

		id, err := sut.GetInt64("id")
		require.NoError(t, err)

		assert.EqualValues(t, cnt, id)

		return false, nil
	}))

	assert.EqualValues(t, 3, cnt)

	require.NoError(t, trx1.Commit())
}

func (ts *SelectScanTestSuite) TestFailIfWrappedScanNotImplementsUpdateScan() {
	t := ts.T()

	mc := minimock.NewController(t)

	ts1 := scan.NewScanMock(mc)
	ts1.HasFieldMock.Return(false)
	ts1.SchemaMock.Return(ts.testLayout().Schema)

	sut := scan.NewSelectScan(ts1,
		scan.NewAndPredicate(
			scan.NewEqualTerm(
				scan.NewFieldExpression("age"),
				scan.NewScalarExpression(scan.NewInt8Constant(2)),
			),
		),
	)

	assert.ErrorIs(t, sut.Insert(), scan.ErrUpdateScanNotImplemented)
	assert.ErrorIs(t, sut.Delete(), scan.ErrUpdateScanNotImplemented)

	assert.ErrorIs(t, sut.MoveToRID(types.RID{}), scan.ErrUpdateScanNotImplemented)
	assert.Equal(t, types.RID{}, sut.RID())

	assert.ErrorIs(t, sut.SetInt64("id", 0), scan.ErrUpdateScanNotImplemented)
	assert.ErrorIs(t, sut.SetInt8("age", 0), scan.ErrUpdateScanNotImplemented)
	assert.ErrorIs(t, sut.SetString("name", ""), scan.ErrUpdateScanNotImplemented)
	assert.ErrorIs(t, sut.SetVal("id", scan.NewInt64Constant(0)), scan.ErrUpdateScanNotImplemented)
}
