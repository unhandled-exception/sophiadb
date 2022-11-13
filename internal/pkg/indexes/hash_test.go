package indexes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type StaticHashIndexTestSuite struct {
	Suite
}

func TestStaticHashIndextestSuite(t *testing.T) {
	suite.Run(t, new(StaticHashIndexTestSuite))
}

func (ts *StaticHashIndexTestSuite) newSUT(indexName string, valueType records.FieldType, length int64) (indexes.Index, *transaction.Transaction, func()) {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, t.TempDir())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	sut, err := indexes.NewStaticHashIndex(trx, indexName, indexes.NewIndexLayout(valueType, length))
	require.NoError(t, err)

	return sut, trx, func() {
		require.NoError(t, trx.Commit())
		require.NoError(t, fm.Close())
	}
}

func (ts *StaticHashIndexTestSuite) TestInt64HashIndex() {
	t := ts.T()

	sut, _, clean := ts.newSUT("table_int64_idx", records.Int64Field, 0)
	defer clean()

	assert.EqualValues(t, 10, sut.SearchCost(1024, 316))

	totalCount := int64(1000)

	for i := int64(0); i < totalCount*5; i++ {
		require.NoError(t, sut.Insert(
			scan.NewInt64Constant(i%totalCount),
			types.RID{
				BlockNumber: types.BlockID(i / totalCount),
				Slot:        types.SlotID(i % totalCount),
			},
		))
	}

	var testValue int64 = 7

	require.NoError(t, sut.BeforeFirst(scan.NewInt64Constant(testValue)))

	cnt := 0

	for {
		ok, err := sut.Next()
		require.NoError(t, err)

		if !ok {
			break
		}

		rid := sut.RID()

		assert.EqualValues(t, cnt, rid.BlockNumber)
		assert.EqualValues(t, testValue, rid.Slot)

		cnt++
	}

	assert.EqualValues(t, 5, cnt)

	require.NoError(t,
		sut.Delete(
			scan.NewInt64Constant(testValue),
			types.RID{
				BlockNumber: 1,
				Slot:        types.SlotID(testValue),
			},
		),
	)

	require.NoError(t,
		sut.Delete(
			scan.NewInt64Constant(testValue),
			types.RID{
				BlockNumber: 1000,
				Slot:        types.SlotID(testValue),
			},
		),
	)

	cnt = 0

	require.NoError(t, sut.BeforeFirst(scan.NewInt64Constant(testValue)))

	for {
		ok, err := sut.Next()
		require.NoError(t, err)

		if !ok {
			break
		}
		cnt++
	}

	assert.EqualValues(t, 4, cnt)
}
