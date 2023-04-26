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

	require.NoError(t, btp.FormatBlock(block, 0))

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
	total := 0

	for _, d := range data {
		for i := 0; i < d.Count; i++ {
			require.NoError(t, page.InsertLeaf(
				types.SlotID(total+i),
				d.SearchKey,
				types.RID{
					BlockNumber: d.FirstRID.BlockNumber + types.BlockID(i),
					Slot:        d.FirstRID.Slot + types.SlotID(i),
				},
			))
		}

		total += d.Count
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
			BlockNumber: types.BlockID(int(firstRID.BlockNumber) + i),
			Slot:        types.SlotID(int(firstRID.Slot) + i),
		}, rid, "i = %d", i)
		assert.EqualValues(t, testPageData[0].Count+i, sut.CurrentSlot())
	}

	ok, err := sut.Next()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func (ts *BTreeLeafTestSuite) TestSplitPageOnInsertToPageWithMultipleValuesAndSplitPage() {
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
			Count:     int(maxRecs / 2),
			FirstRID: types.RID{
				BlockNumber: 235,
				Slot:        350,
			},
		},
		{
			SearchKey: scan.NewInt64Constant(searchVal + 13),
			Count:     int((maxRecs - maxRecs/2) - 1),
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

	de, err := sut.Insert(types.RID{
		BlockNumber: types.BlockID(int(firstRID.BlockNumber) + 1),
		Slot:        types.SlotID(int(firstRID.Slot) + 1),
	})
	assert.NoError(t, err)
	assert.Nil(t, de)

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
