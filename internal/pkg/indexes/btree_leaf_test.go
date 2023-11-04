package indexes_test

import (
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

const (
	leafPageDataOffset = 8
)

type BTreeLeafTestSuite struct {
	Suite
}

func TestBTreeLeaftestSuite(t *testing.T) {
	suite.Run(t, new(BTreeLeafTestSuite))
}

func (ts *BTreeLeafTestSuite) newSUT(layout records.Layout, searchkey scan.Constant, data []btreeLeafTestsPageData) (*indexes.BTreeLeaf, *indexes.BTreePage, *transaction.Transaction, *storage.Manager) {
	t := ts.T()

	testFile := "btleaf_test.dat"

	trxMan, fm := ts.newTRXManager(defaultLockTimeout, t.TempDir())

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	block, err := fm.Append(testFile)
	require.NoError(t, err)

	btp, err := indexes.NewBTreePage(trx, block, layout)
	require.NoError(t, err)

	require.NoError(t, btp.FormatBlock(block, -1))

	ts.fillTestPage(btp, data)

	btl, err := indexes.NewBTreeLeaf(trx, block, layout, searchkey)
	require.NoError(t, err)

	return btl, btp, trx, fm
}

type btreeLeafTestsPageData struct {
	SearchKey scan.Constant
	Count     int
	FirstRID  types.RID
}

func (ts *BTreeLeafTestSuite) fillTestPage(page *indexes.BTreePage, data []btreeLeafTestsPageData) {
	t := ts.T()
	slot := -1

	for _, d := range data {
		for i := 0; i < d.Count; i++ {
			slot += 1
			require.NoError(t, page.InsertLeaf(
				types.SlotID(slot),
				d.SearchKey,
				types.RID{
					BlockNumber: d.FirstRID.BlockNumber + types.BlockID(i),
					Slot:        d.FirstRID.Slot + types.SlotID(i),
				},
			))
		}
	}
}

func (ts *BTreeLeafTestSuite) testDataRecords(data []btreeLeafTestsPageData) int {
	cnt := 0

	for _, d := range data {
		cnt += d.Count
	}

	return cnt
}

func (ts *BTreeLeafTestSuite) TestInsertWithoutOverflow() {
	t := ts.T()

	layout := indexes.NewBTreeLeafPageLayout(records.Int64Field, 0)

	searchVal := int64(33)
	searchKey := scan.NewInt64Constant(searchVal)

	searchKeyCount := 15

	firstRID := types.RID{
		BlockNumber: 145,
		Slot:        0,
	}

	testPageData := []btreeLeafTestsPageData{
		{
			SearchKey: scan.NewInt64Constant(searchVal - 13),
			Count:     25,
			FirstRID: types.RID{
				BlockNumber: 235,
				Slot:        350,
			},
		},
		{
			SearchKey: scan.NewInt64Constant(searchVal + 13),
			Count:     27,
			FirstRID: types.RID{
				BlockNumber: 335,
				Slot:        150,
			},
		},
	}

	sut, page, _, fm := ts.newSUT(layout, searchKey, testPageData)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	for i := 0; i < searchKeyCount; i++ {
		require.NoError(t, sut.ResetSlot())
		de, err := sut.Insert(types.RID{
			BlockNumber: types.BlockID(int(firstRID.BlockNumber) + i),
			Slot:        types.SlotID(int(firstRID.Slot) + i),
		})
		assert.NoErrorf(t, err, "i = %d", i)
		assert.Nil(t, de, "i = %d", i)
	}

	recs, err := page.GetRecords()
	require.NoError(t, err)
	require.EqualValues(t, recs, ts.testDataRecords(testPageData)+searchKeyCount)

	require.NoError(t, sut.ResetSlot())

	for i := 0; i < searchKeyCount; i++ {
		ok, err1 := sut.Next()
		assert.NoError(t, err1)
		assert.Truef(t, ok, "i = %d", i)

		rid, err1 := sut.RID()
		require.NoError(t, err1)
		assert.Equalf(t, types.RID{
			BlockNumber: types.BlockID(int(firstRID.BlockNumber) + searchKeyCount - i - 1),
			Slot:        types.SlotID(int(firstRID.Slot) + searchKeyCount - i - 1),
		}, rid, "i = %d", i)
		assert.EqualValues(t, testPageData[0].Count+i, sut.CurrentSlot())
	}

	ok, err := sut.Next()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func (ts *BTreeLeafTestSuite) TestSplitPageOnInsertToPageWithMultipleValues() {
	t := ts.T()

	layout := indexes.NewBTreeLeafPageLayout(records.Int64Field, 0)

	searchVal := int64(33)
	searchKey := scan.NewInt64Constant(searchVal)

	firstRID := types.RID{
		BlockNumber: 145,
		Slot:        0,
	}

	maxRecs := (defaultTestBlockSize - leafPageDataOffset) / layout.SlotSize

	testPageData := []btreeLeafTestsPageData{
		{
			SearchKey: scan.NewInt64Constant(searchVal - 13),
			Count:     int(maxRecs / 4),
			FirstRID: types.RID{
				BlockNumber: 235,
				Slot:        350,
			},
		},
		{
			SearchKey: scan.NewInt64Constant(searchVal),
			Count:     int((maxRecs/2 - maxRecs/4)),
			FirstRID: types.RID{
				BlockNumber: 235,
				Slot:        350,
			},
		},
		{
			SearchKey: scan.NewInt64Constant(searchVal + 13),
			Count:     int((maxRecs - maxRecs/2) - 2),
			FirstRID: types.RID{
				BlockNumber: 335,
				Slot:        150,
			},
		},
	}

	sut, page, _, fm := ts.newSUT(layout, searchKey, testPageData)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	require.NoError(t, sut.ResetSlot())

	de, err := sut.Insert(types.RID{
		BlockNumber: types.BlockID(int(firstRID.BlockNumber) + 1),
		Slot:        types.SlotID(int(firstRID.Slot) + 1),
	})
	assert.NoError(t, err)
	assert.Nil(t, de)

	require.NoError(t, sut.ResetSlot())

	de, err = sut.Insert(types.RID{
		BlockNumber: types.BlockID(int(firstRID.BlockNumber) + 2),
		Slot:        types.SlotID(int(firstRID.Slot) + 2),
	})
	assert.NoError(t, err)
	assert.Equal(t, &indexes.BTreeDirEntry{
		BlockNumber: 1,
		Dataval:     searchKey,
	}, de)

	recs, err := page.GetRecords()
	require.NoError(t, err)
	require.EqualValues(t, testPageData[0].Count, recs)

	require.NoError(t, sut.ResetSlot())

	ok, err := sut.Next()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func (ts *BTreeLeafTestSuite) TestSplitPageOnInsertToPageWithKeyGreaterMedian() {
	t := ts.T()

	layout := indexes.NewBTreeLeafPageLayout(records.Int64Field, 0)

	searchVal := int64(33)
	searchKey := scan.NewInt64Constant(searchVal)

	firstRID := types.RID{
		BlockNumber: 145,
		Slot:        0,
	}

	maxRecs := (defaultTestBlockSize - leafPageDataOffset) / layout.SlotSize

	testPageData := []btreeLeafTestsPageData{
		{
			SearchKey: scan.NewInt64Constant(searchVal - 13),
			Count:     int(maxRecs * 3 / 5),
			FirstRID: types.RID{
				BlockNumber: 235,
				Slot:        350,
			},
		},
		{
			SearchKey: scan.NewInt64Constant(searchVal),
			Count:     int(maxRecs - maxRecs*3/5 - 2),
			FirstRID: types.RID{
				BlockNumber: 235,
				Slot:        350,
			},
		},
	}

	sut, page, _, fm := ts.newSUT(layout, searchKey, testPageData)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	require.NoError(t, sut.ResetSlot())

	de, err := sut.Insert(types.RID{
		BlockNumber: types.BlockID(int(firstRID.BlockNumber) + 1),
		Slot:        types.SlotID(int(firstRID.Slot) + 1),
	})
	assert.NoError(t, err)
	assert.Nil(t, de)

	require.NoError(t, sut.ResetSlot())

	de, err = sut.Insert(types.RID{
		BlockNumber: types.BlockID(int(firstRID.BlockNumber) + 2),
		Slot:        types.SlotID(int(firstRID.Slot) + 2),
	})
	assert.NoError(t, err)
	assert.Equal(t, &indexes.BTreeDirEntry{
		BlockNumber: 1,
		Dataval:     searchKey,
	}, de)

	recs, err := page.GetRecords()
	require.NoError(t, err)
	require.EqualValues(t, testPageData[0].Count, recs)

	require.NoError(t, sut.ResetSlot())

	ok, err := sut.Next()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func (ts *BTreeLeafTestSuite) TestSplitPageOnInsertToPageWithSingleValue() {
	t := ts.T()

	layout := indexes.NewBTreeLeafPageLayout(records.Int64Field, 0)

	searchVal := int64(33)
	searchKey := scan.NewInt64Constant(searchVal)

	firstRID := types.RID{
		BlockNumber: 145,
		Slot:        0,
	}

	maxRecs := (defaultTestBlockSize - leafPageDataOffset) / layout.SlotSize

	testPageData := []btreeLeafTestsPageData{
		{
			SearchKey: scan.NewInt64Constant(searchVal),
			Count:     int(maxRecs) - 1,
			FirstRID:  firstRID,
		},
	}

	sut, page, _, fm := ts.newSUT(layout, searchKey, testPageData)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	require.NoError(t, sut.ResetSlot())

	de, err := sut.Insert(types.RID{
		BlockNumber: types.BlockID(int(firstRID.BlockNumber) + 2),
		Slot:        types.SlotID(int(firstRID.Slot) + 2),
	})
	assert.NoError(t, err)
	assert.Nil(t, de)

	recs, err := page.GetRecords()
	require.NoError(t, err)
	require.EqualValues(t, 1, recs)

	require.NoError(t, sut.ResetSlot())

	for i := 0; i < int(maxRecs); i++ {
		ok, err := sut.Next()
		assert.NoError(t, err)
		assert.True(t, ok)
	}
}

func (ts *BTreeLeafTestSuite) TestSplitPageOnInsertToPageWithOverflowBlock() {
	t := ts.T()

	layout := indexes.NewBTreeLeafPageLayout(records.Int64Field, 0)

	searchVal := int64(33)
	searchKey := scan.NewInt64Constant(searchVal)

	firstRID := types.RID{
		BlockNumber: 145,
		Slot:        0,
	}

	maxRecs := (defaultTestBlockSize - leafPageDataOffset) / layout.SlotSize

	testPageData := []btreeLeafTestsPageData{
		{
			SearchKey: scan.NewInt64Constant(searchVal + 13),
			Count:     int(maxRecs),
			FirstRID: types.RID{
				BlockNumber: 335,
				Slot:        150,
			},
		},
	}

	sut, page, _, fm := ts.newSUT(layout, searchKey, testPageData)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	require.NoError(t, page.SetFlag(100))

	require.NoError(t, sut.ResetSlot())

	de, err := sut.Insert(types.RID{
		BlockNumber: types.BlockID(int(firstRID.BlockNumber) + 1),
		Slot:        types.SlotID(int(firstRID.Slot) + 1),
	})
	assert.NoError(t, err)
	assert.Equal(t,
		&indexes.BTreeDirEntry{
			BlockNumber: 1,
			Dataval:     searchKey,
		},
		de,
	)

	recs, err := page.GetRecords()
	require.NoError(t, err)
	require.EqualValues(t, 1, recs)

	require.NoError(t, sut.ResetSlot())

	ok, err := sut.Next()
	assert.NoError(t, err)
	assert.True(t, ok)
}

func (ts *BTreeLeafTestSuite) TestDelete() {
	t := ts.T()

	layout := indexes.NewBTreeLeafPageLayout(records.Int64Field, 0)

	searchVal := int64(33)
	searchKey := scan.NewInt64Constant(searchVal)

	firstRID := types.RID{
		BlockNumber: 145,
		Slot:        0,
	}

	maxRecs := (defaultTestBlockSize - leafPageDataOffset) / layout.SlotSize
	testRecsCount := (maxRecs/2 - maxRecs/4)

	testPageData := []btreeLeafTestsPageData{
		{
			SearchKey: scan.NewInt64Constant(searchVal),
			Count:     int((maxRecs - testRecsCount) / 2),
			FirstRID: types.RID{
				BlockNumber: 235,
				Slot:        350,
			},
		},
		{
			SearchKey: scan.NewInt64Constant(searchVal),
			Count:     int(testRecsCount),
			FirstRID:  firstRID,
		},
		{
			SearchKey: scan.NewInt64Constant(searchVal),
			Count:     int(maxRecs - (maxRecs-testRecsCount)/2 - testRecsCount),
			FirstRID: types.RID{
				BlockNumber: 335,
				Slot:        150,
			},
		},
	}

	sut, page, _, fm := ts.newSUT(layout, searchKey, testPageData)
	defer sut.Close()
	defer func() {
		require.NoError(t, fm.Close())
	}()

	for i := 0; i < int(testRecsCount); i++ {
		require.NoError(t, sut.ResetSlot())

		ok, err := sut.Delete(types.RID{
			BlockNumber: firstRID.BlockNumber + types.BlockID(i),
			Slot:        firstRID.Slot + types.SlotID(i),
		})
		require.NoError(t, err)
		require.True(t, ok)
	}

	recs, err := page.GetRecords()
	require.NoError(t, err)
	require.EqualValues(t, ts.testDataRecords(testPageData)-int(testRecsCount), recs)

	require.NoError(t, sut.ResetSlot())

	ok, err := sut.Next()
	assert.NoError(t, err)
	assert.True(t, ok)
}
