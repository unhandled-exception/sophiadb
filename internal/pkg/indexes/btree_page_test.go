package indexes_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type BTreePageTestSuite struct {
	Suite
}

func TestBTreePagetestSuite(t *testing.T) {
	suite.Run(t, new(BTreePageTestSuite))
}

const defaultTestFlag = 0x0f

func (ts *BTreePageTestSuite) newSUT(layout records.Layout, flag int64) (*indexes.BTreePage, *transaction.Transaction, *storage.Manager) {
	t := ts.T()

	testFile := "btpage_test.dat"

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, t.TempDir())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	block, err := fm.Append(testFile)
	require.NoError(t, err)

	btp, err := indexes.NewBTreePage(trx, block, layout)
	require.NoError(t, err)

	require.NoError(t, btp.FormatBlock(block, flag))

	return btp, trx, fm
}

func (ts *BTreePageTestSuite) TestFlag() {
	t := ts.T()

	sut, _, fm := ts.newSUT(indexes.NewBTreeLeafPageLayout(records.Int64Field, 0), defaultTestFlag)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	flag, err := sut.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, defaultTestFlag, flag)

	testFlag := int64(0xf0f04578010101)
	require.NoError(t, sut.SetFlag(testFlag))

	flag, err = sut.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, testFlag, flag)
}

func (ts *BTreePageTestSuite) TestLeafPageIsFull() {
	t := ts.T()

	layout := indexes.NewBTreeLeafPageLayout(records.Int64Field, 0)

	sut, trx, fm := ts.newSUT(layout, defaultTestFlag)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := int(trx.BlockSize()/layout.SlotSize) - 1

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut.InsertLeaf(
			types.SlotID(i),
			scan.NewInt64Constant(int64(i)),
			types.RID{
				BlockNumber: types.BlockID(i),
				Slot:        types.SlotID(i),
			},
		))
	}

	isFull, err := sut.IsFull()
	require.NoError(t, err)
	assert.True(t, isFull)
}

func (ts *BTreePageTestSuite) TestInt64LeafPage() {
	t := ts.T()

	sut, _, fm := ts.newSUT(indexes.NewBTreeLeafPageLayout(records.Int64Field, 0), defaultTestFlag)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 20
	startBlock := 32
	lastValue := 9600

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut.InsertLeaf(
			0,
			scan.NewInt64Constant(int64(lastValue-i)),
			types.RID{
				BlockNumber: types.BlockID(startBlock + i),
				Slot:        types.SlotID(i),
			},
		))
	}

	recs, err := sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt, recs)

	for i := 0; i < cnt; i++ {
		rid, err1 := sut.GetDataRID(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.RID{BlockNumber: types.BlockID(startBlock + cnt - i - 1), Slot: types.SlotID(cnt - i - 1)}, rid)

		val, err1 := sut.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt64Constant(int64(lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	slot, err := sut.FindSlotBefore(scan.NewInt64Constant(10))
	require.NoError(t, err)
	assert.EqualValues(t, -1, slot)

	slot, err = sut.FindSlotBefore(scan.NewInt64Constant(int64(lastValue - 10)))
	require.NoError(t, err)
	assert.EqualValues(t, 8, slot)

	require.NoError(t, sut.Delete(0))
	require.NoError(t, sut.Delete(0))

	recs, err = sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-2, recs)

	slot, err = sut.FindSlotBefore(scan.NewInt64Constant(int64(lastValue - cnt + 1 + 4)))
	require.NoError(t, err)
	assert.EqualValues(t, 1, slot)
}

func (ts *BTreePageTestSuite) TestSplitInt64LeafPage() {
	t := ts.T()

	layout := indexes.NewBTreeLeafPageLayout(records.Int64Field, 0)

	sut1, trx, fm := ts.newSUT(layout, defaultTestFlag)
	defer sut1.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 25
	splitPos := 15

	startBlock := 32
	lastValue := 9600

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut1.InsertLeaf(
			0,
			scan.NewInt64Constant(int64(lastValue-i)),
			types.RID{
				BlockNumber: types.BlockID(startBlock + i),
				Slot:        types.SlotID(i),
			},
		))
	}

	newTestFlag := int64(defaultTestFlag + 1)
	newBlock, err := sut1.Split(types.SlotID(splitPos), newTestFlag)
	require.NoError(t, err)

	sut2, err := indexes.NewBTreePage(trx, newBlock, layout)
	require.NoError(t, err)

	recs1, err := sut1.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, splitPos, recs1)

	flag1, err := sut1.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, defaultTestFlag, flag1)

	recs2, err := sut2.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-splitPos, recs2)

	flag2, err := sut2.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, newTestFlag, flag2)

	for i := 0; i < int(recs1); i++ {
		rid, err1 := sut1.GetDataRID(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.RID{BlockNumber: types.BlockID(startBlock + cnt - i - 1), Slot: types.SlotID(cnt - i - 1)}, rid)

		val, err1 := sut1.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt64Constant(int64(lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	for i := 0; i < int(recs2); i++ {
		rid, err1 := sut2.GetDataRID(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.RID{BlockNumber: types.BlockID(startBlock + cnt - splitPos - 1 - i), Slot: types.SlotID(cnt - splitPos - 1 - i)}, rid)

		val, err1 := sut2.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt64Constant(int64(lastValue-cnt+splitPos+i+1))), "%s != %d", val.String(), lastValue-cnt+splitPos+i+1)
	}
}

func (ts *BTreePageTestSuite) TestInt8LeafPage() {
	t := ts.T()

	sut, _, fm := ts.newSUT(indexes.NewBTreeLeafPageLayout(records.Int8Field, 0), defaultTestFlag)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 20
	startBlock := 32
	lastValue := 120

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut.InsertLeaf(
			0,
			scan.NewInt8Constant(int8(lastValue-i)),
			types.RID{
				BlockNumber: types.BlockID(startBlock + i),
				Slot:        types.SlotID(i),
			},
		))
	}

	recs, err := sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt, recs)

	for i := 0; i < cnt; i++ {
		rid, err1 := sut.GetDataRID(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.RID{BlockNumber: types.BlockID(startBlock + cnt - i - 1), Slot: types.SlotID(cnt - i - 1)}, rid)

		val, err1 := sut.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt8Constant(int8(lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	slot, err := sut.FindSlotBefore(scan.NewInt8Constant(10))
	require.NoError(t, err)
	assert.EqualValues(t, -1, slot)

	slot, err = sut.FindSlotBefore(scan.NewInt8Constant(int8(lastValue - 10)))
	require.NoError(t, err)
	assert.EqualValues(t, 8, slot)

	require.NoError(t, sut.Delete(0))
	require.NoError(t, sut.Delete(0))

	recs, err = sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-2, recs)

	slot, err = sut.FindSlotBefore(scan.NewInt8Constant(int8(lastValue - cnt + 1 + 4)))
	require.NoError(t, err)
	assert.EqualValues(t, 1, slot)
}

func (ts *BTreePageTestSuite) TestSplitInt8LeafPage() {
	t := ts.T()

	layout := indexes.NewBTreeLeafPageLayout(records.Int8Field, 0)

	sut1, trx, fm := ts.newSUT(layout, defaultTestFlag)
	defer sut1.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 25
	splitPos := 15

	startBlock := 32
	lastValue := 120

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut1.InsertLeaf(
			0,
			scan.NewInt8Constant(int8(lastValue-i)),
			types.RID{
				BlockNumber: types.BlockID(startBlock + i),
				Slot:        types.SlotID(i),
			},
		))
	}

	newTestFlag := int64(defaultTestFlag + 1)
	newBlock, err := sut1.Split(types.SlotID(splitPos), newTestFlag)
	require.NoError(t, err)

	sut2, err := indexes.NewBTreePage(trx, newBlock, layout)
	require.NoError(t, err)

	recs1, err := sut1.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, splitPos, recs1)

	flag1, err := sut1.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, defaultTestFlag, flag1)

	recs2, err := sut2.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-splitPos, recs2)

	flag2, err := sut2.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, newTestFlag, flag2)

	for i := 0; i < int(recs1); i++ {
		rid, err1 := sut1.GetDataRID(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.RID{BlockNumber: types.BlockID(startBlock + cnt - i - 1), Slot: types.SlotID(cnt - i - 1)}, rid)

		val, err1 := sut1.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt8Constant(int8(lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	for i := 0; i < int(recs2); i++ {
		rid, err1 := sut2.GetDataRID(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.RID{BlockNumber: types.BlockID(startBlock + cnt - splitPos - 1 - i), Slot: types.SlotID(cnt - splitPos - 1 - i)}, rid)

		val, err1 := sut2.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt8Constant(int8(lastValue-cnt+splitPos+i+1))), "%s != %d", val.String(), lastValue-cnt+splitPos+i+1)
	}
}

func (ts *BTreePageTestSuite) TestStringLeafPage() {
	t := ts.T()

	sut, _, fm := ts.newSUT(indexes.NewBTreeLeafPageLayout(records.StringField, 20), defaultTestFlag)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 20
	startBlock := 32
	lastValue := 9600

	valFmt := "val_%02d" //nolint:goconst

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut.InsertLeaf(
			0,
			scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-i)),
			types.RID{
				BlockNumber: types.BlockID(startBlock + i),
				Slot:        types.SlotID(i),
			},
		))
	}

	recs, err := sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt, recs)

	for i := 0; i < cnt; i++ {
		rid, err1 := sut.GetDataRID(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.RID{BlockNumber: types.BlockID(startBlock + cnt - i - 1), Slot: types.SlotID(cnt - i - 1)}, rid)

		val, err1 := sut.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	slot, err := sut.FindSlotBefore(scan.NewStringConstant(fmt.Sprintf(valFmt, 10)))
	require.NoError(t, err)
	assert.EqualValues(t, -1, slot)

	slot, err = sut.FindSlotBefore(scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-10)))
	require.NoError(t, err)
	assert.EqualValues(t, 8, slot)

	require.NoError(t, sut.Delete(0))
	require.NoError(t, sut.Delete(0))

	recs, err = sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-2, recs)

	slot, err = sut.FindSlotBefore(scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-cnt+1+4)))
	require.NoError(t, err)
	assert.EqualValues(t, 1, slot)
}

func (ts *BTreePageTestSuite) TestSplitStringLeafPage() {
	t := ts.T()

	layout := indexes.NewBTreeLeafPageLayout(records.StringField, 20)

	sut1, trx, fm := ts.newSUT(layout, defaultTestFlag)
	defer sut1.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 25
	splitPos := 15

	startBlock := 32
	lastValue := 9600

	valFmt := "val_%02d" //nolint:goconst

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut1.InsertLeaf(
			0,
			scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-i)),
			types.RID{
				BlockNumber: types.BlockID(startBlock + i),
				Slot:        types.SlotID(i),
			},
		))
	}

	newTestFlag := int64(defaultTestFlag + 1)
	newBlock, err := sut1.Split(types.SlotID(splitPos), newTestFlag)
	require.NoError(t, err)

	sut2, err := indexes.NewBTreePage(trx, newBlock, layout)
	require.NoError(t, err)

	recs1, err := sut1.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, splitPos, recs1)

	flag1, err := sut1.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, defaultTestFlag, flag1)

	recs2, err := sut2.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-splitPos, recs2)

	flag2, err := sut2.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, newTestFlag, flag2)

	for i := 0; i < int(recs1); i++ {
		rid, err1 := sut1.GetDataRID(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.RID{BlockNumber: types.BlockID(startBlock + cnt - i - 1), Slot: types.SlotID(cnt - i - 1)}, rid)

		val, err1 := sut1.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	for i := 0; i < int(recs2); i++ {
		rid, err1 := sut2.GetDataRID(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.RID{BlockNumber: types.BlockID(startBlock + cnt - splitPos - 1 - i), Slot: types.SlotID(cnt - splitPos - 1 - i)}, rid)

		val, err1 := sut2.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-cnt+splitPos+i+1))), "%s != %d", val.String(), lastValue-cnt+splitPos+i+1)
	}
}

func (ts *BTreePageTestSuite) TestDirPageIsFull() {
	t := ts.T()

	layout := indexes.NewBTreeDirPageLayout(records.Int64Field, 0)

	sut, trx, fm := ts.newSUT(layout, defaultTestFlag)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := int(trx.BlockSize()/layout.SlotSize) - 1

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut.InsertDir(
			types.SlotID(i),
			scan.NewInt64Constant(int64(i)),
			types.BlockID(i),
		))
	}

	isFull, err := sut.IsFull()
	require.NoError(t, err)
	assert.True(t, isFull)
}

func (ts *BTreePageTestSuite) TestInt64DirPage() {
	t := ts.T()

	sut, _, fm := ts.newSUT(indexes.NewBTreeDirPageLayout(records.Int64Field, 0), defaultTestFlag)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 20
	startBlock := 32
	lastValue := 9600

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut.InsertDir(
			0,
			scan.NewInt64Constant(int64(lastValue-i)),
			types.BlockID(startBlock+i),
		))
	}

	recs, err := sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt, recs)

	for i := 0; i < cnt; i++ {
		blockID, err1 := sut.GetChildNum(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.BlockID(startBlock+cnt-i-1), blockID)

		val, err1 := sut.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt64Constant(int64(lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	slot, err := sut.FindSlotBefore(scan.NewInt64Constant(10))
	require.NoError(t, err)
	assert.EqualValues(t, -1, slot)

	slot, err = sut.FindSlotBefore(scan.NewInt64Constant(int64(lastValue - 10)))
	require.NoError(t, err)
	assert.EqualValues(t, 8, slot)

	require.NoError(t, sut.Delete(0))
	require.NoError(t, sut.Delete(0))

	recs, err = sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-2, recs)

	slot, err = sut.FindSlotBefore(scan.NewInt64Constant(int64(lastValue - cnt + 1 + 4)))
	require.NoError(t, err)
	assert.EqualValues(t, 1, slot)
}

func (ts *BTreePageTestSuite) TestSplitInt64DirPage() {
	t := ts.T()

	layout := indexes.NewBTreeDirPageLayout(records.Int64Field, 0)

	sut1, trx, fm := ts.newSUT(layout, defaultTestFlag)
	defer sut1.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 25
	splitPos := 15

	startBlock := 32
	lastValue := 9600

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut1.InsertDir(
			0,
			scan.NewInt64Constant(int64(lastValue-i)),
			types.BlockID(startBlock+i),
		))
	}

	newTestFlag := int64(defaultTestFlag + 1)
	newBlock, err := sut1.Split(types.SlotID(splitPos), newTestFlag)
	require.NoError(t, err)

	sut2, err := indexes.NewBTreePage(trx, newBlock, layout)
	require.NoError(t, err)

	recs1, err := sut1.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, splitPos, recs1)

	flag1, err := sut1.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, defaultTestFlag, flag1)

	recs2, err := sut2.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-splitPos, recs2)

	flag2, err := sut2.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, newTestFlag, flag2)

	for i := 0; i < int(recs1); i++ {
		blockID, err1 := sut1.GetChildNum(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.BlockID(startBlock+cnt-i-1), blockID)

		val, err1 := sut1.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt64Constant(int64(lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	for i := 0; i < int(recs2); i++ {
		blockID, err1 := sut2.GetChildNum(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.BlockID(startBlock+cnt-splitPos-1-i), blockID)

		val, err1 := sut2.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt64Constant(int64(lastValue-cnt+splitPos+i+1))), "%s != %d", val.String(), lastValue-cnt+splitPos+i+1)
	}
}

func (ts *BTreePageTestSuite) TestInt8DirPage() {
	t := ts.T()

	sut, _, fm := ts.newSUT(indexes.NewBTreeDirPageLayout(records.Int8Field, 0), defaultTestFlag)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 20
	startBlock := 32
	lastValue := 120

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut.InsertDir(
			0,
			scan.NewInt8Constant(int8(lastValue-i)),
			types.BlockID(startBlock+i),
		))
	}

	recs, err := sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt, recs)

	for i := 0; i < cnt; i++ {
		blockID, err1 := sut.GetChildNum(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.BlockID(startBlock+cnt-i-1), blockID)

		val, err1 := sut.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt8Constant(int8(lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	slot, err := sut.FindSlotBefore(scan.NewInt8Constant(10))
	require.NoError(t, err)
	assert.EqualValues(t, -1, slot)

	slot, err = sut.FindSlotBefore(scan.NewInt8Constant(int8(lastValue - 10)))
	require.NoError(t, err)
	assert.EqualValues(t, 8, slot)

	require.NoError(t, sut.Delete(0))
	require.NoError(t, sut.Delete(0))

	recs, err = sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-2, recs)

	slot, err = sut.FindSlotBefore(scan.NewInt8Constant(int8(lastValue - cnt + 1 + 4)))
	require.NoError(t, err)
	assert.EqualValues(t, 1, slot)
}

func (ts *BTreePageTestSuite) TestSplitInt8DirPage() {
	t := ts.T()

	layout := indexes.NewBTreeDirPageLayout(records.Int8Field, 0)

	sut1, trx, fm := ts.newSUT(layout, defaultTestFlag)
	defer sut1.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 25
	splitPos := 15

	startBlock := 32
	lastValue := 120

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut1.InsertDir(
			0,
			scan.NewInt8Constant(int8(lastValue-i)),
			types.BlockID(startBlock+i),
		))
	}

	newTestFlag := int64(defaultTestFlag + 1)
	newBlock, err := sut1.Split(types.SlotID(splitPos), newTestFlag)
	require.NoError(t, err)

	sut2, err := indexes.NewBTreePage(trx, newBlock, layout)
	require.NoError(t, err)

	recs1, err := sut1.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, splitPos, recs1)

	flag1, err := sut1.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, defaultTestFlag, flag1)

	recs2, err := sut2.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-splitPos, recs2)

	flag2, err := sut2.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, newTestFlag, flag2)

	for i := 0; i < int(recs1); i++ {
		blockID, err1 := sut1.GetChildNum(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.BlockID(startBlock+cnt-i-1), blockID)

		val, err1 := sut1.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt8Constant(int8(lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	for i := 0; i < int(recs2); i++ {
		blockID, err1 := sut2.GetChildNum(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.BlockID(startBlock+cnt-splitPos-1-i), blockID)

		val, err1 := sut2.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewInt8Constant(int8(lastValue-cnt+splitPos+i+1))), "%s != %d", val.String(), lastValue-cnt+splitPos+i+1)
	}
}

func (ts *BTreePageTestSuite) TestStringDirPage() {
	t := ts.T()

	sut, _, fm := ts.newSUT(indexes.NewBTreeDirPageLayout(records.StringField, 20), defaultTestFlag)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 20
	startBlock := 32
	lastValue := 9600

	valFmt := "val_%02d" //nolint:goconst

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut.InsertDir(
			0,
			scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-i)),
			types.BlockID(startBlock+i),
		))
	}

	recs, err := sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt, recs)

	for i := 0; i < cnt; i++ {
		blockID, err1 := sut.GetChildNum(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.BlockID(startBlock+cnt-i-1), blockID)

		val, err1 := sut.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	slot, err := sut.FindSlotBefore(scan.NewStringConstant(fmt.Sprintf(valFmt, 10)))
	require.NoError(t, err)
	assert.EqualValues(t, -1, slot)

	slot, err = sut.FindSlotBefore(scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-10)))
	require.NoError(t, err)
	assert.EqualValues(t, 8, slot)

	require.NoError(t, sut.Delete(0))
	require.NoError(t, sut.Delete(0))

	recs, err = sut.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-2, recs)

	slot, err = sut.FindSlotBefore(scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-cnt+1+4)))
	require.NoError(t, err)
	assert.EqualValues(t, 1, slot)
}

func (ts *BTreePageTestSuite) TestSplitStringDirPage() {
	t := ts.T()

	layout := indexes.NewBTreeDirPageLayout(records.StringField, 20)

	sut1, trx, fm := ts.newSUT(layout, defaultTestFlag)
	defer sut1.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	cnt := 25
	splitPos := 15

	startBlock := 32
	lastValue := 9600

	valFmt := "val_%02d" //nolint:goconst

	for i := 0; i < cnt; i++ {
		require.NoError(t, sut1.InsertDir(
			0,
			scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-i)),
			types.BlockID(startBlock+i),
		))
	}

	newTestFlag := int64(defaultTestFlag + 1)
	newBlock, err := sut1.Split(types.SlotID(splitPos), newTestFlag)
	require.NoError(t, err)

	sut2, err := indexes.NewBTreePage(trx, newBlock, layout)
	require.NoError(t, err)

	recs1, err := sut1.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, splitPos, recs1)

	flag1, err := sut1.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, defaultTestFlag, flag1)

	recs2, err := sut2.GetRecords()
	require.NoError(t, err)
	assert.EqualValues(t, cnt-splitPos, recs2)

	flag2, err := sut2.GetFlag()
	require.NoError(t, err)
	assert.EqualValues(t, newTestFlag, flag2)

	for i := 0; i < int(recs1); i++ {
		blockID, err1 := sut1.GetChildNum(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.BlockID(startBlock+cnt-i-1), blockID)

		val, err1 := sut1.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-cnt+i+1))), "%s != %d", val.String(), lastValue-cnt+i+1)
	}

	for i := 0; i < int(recs2); i++ {
		blockID, err1 := sut2.GetChildNum(types.SlotID(i))
		require.NoError(t, err1)
		assert.Equal(t, types.BlockID(startBlock+cnt-splitPos-1-i), blockID)

		val, err1 := sut2.GetVal(types.SlotID(i))
		require.NoError(t, err1)
		assert.EqualValuesf(t, scan.CompEqual, val.CompareTo(scan.NewStringConstant(fmt.Sprintf(valFmt, lastValue-cnt+splitPos+i+1))), "%s != %d", val.String(), lastValue-cnt+splitPos+i+1)
	}
}
