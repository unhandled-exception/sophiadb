package records_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

const (
	testDataFile              = "data.dat"
	testWALFile               = "record_page_wal.dat"
	defaultTestBlockSize      = 4000
	defaultTestBuffersPoolLen = 100
	defaultLockTimeout        = 100 * time.Millisecond
)

type RecordPageTestSuite struct {
	testutil.Suite
}

func TestRecordPageTestsuite(t *testing.T) {
	suite.Run(t, new(RecordPageTestSuite))
}

func (ts *RecordPageTestSuite) newTRXManager(lockTimeout time.Duration) (*transaction.TRXManager, *storage.Manager) {
	path := ts.CreateTestTemporaryDir()

	fm, err := storage.NewFileManager(path, defaultTestBlockSize)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	lm, err := wal.NewManager(fm, testWALFile)
	ts.Require().NoError(err)
	ts.Require().FileExists(filepath.Join(path, testWALFile))

	bm := buffers.NewManager(fm, lm, defaultTestBuffersPoolLen)

	m := transaction.NewTRXManager(fm, bm, lm, transaction.WithLockTimeout(lockTimeout))

	return m, fm
}

func (ts *RecordPageTestSuite) fetchWAL(t *testing.T, trxMan *transaction.TRXManager) []string {
	it, err := trxMan.LogManager().Iterator()
	require.NoError(t, err)

	result := make([]string, 0)

	for it.HasNext() {
		raw, err := it.Next()
		require.NoError(t, err)

		lr, err := recovery.NewLogRecordFromBytes(raw)
		require.NoError(t, err)

		result = append([]string{lr.String()}, result...)
	}

	return result
}

func (ts *RecordPageTestSuite) testLayout() records.Layout {
	schema := records.NewSchema()
	schema.AddInt64Field("id")
	schema.AddStringField("name", 25)
	schema.AddInt8Field("age")

	return records.NewLayout(schema)
}

func (ts *RecordPageTestSuite) newTestRecordPage(t *testing.T) (*records.RecordPage, *transaction.Transaction, func()) {
	trxMan, fm := ts.newTRXManager(defaultLockTimeout)

	trx, err := trxMan.Transaction()
	require.NoError(t, err)

	block, err := trx.Append(testDataFile)
	require.NoError(t, err)

	require.NoError(t, trx.Pin(block))

	layout := ts.testLayout()

	sut, err := records.NewRecordPage(trx, block, layout)
	require.NoError(t, err)

	formatedSlots, err := sut.Format()
	require.NoError(t, err)
	assert.Greater(t, formatedSlots, int32(0))
	assert.EqualValues(t, defaultTestBlockSize/layout.SlotSize, formatedSlots)

	assert.Equal(t, []string{"<START, 1>"}, ts.fetchWAL(t, trxMan))

	return sut, trx, func() {
		fm.Close()
	}
}

func (ts *RecordPageTestSuite) TestRecordPage() {
	t := ts.T()

	sut, trx, clean := ts.newTestRecordPage(t)
	defer clean()

	cnt := 20

	for i := 0; i < cnt; i++ {
		slot, err := sut.InsertAfter(types.SlotID(i - 1))
		require.NoError(t, err)

		require.NoError(t, sut.SetInt64(slot, "id", int64(slot+1)))
		require.NoError(t, sut.SetInt8(slot, "age", int8(slot+2)))
		require.NoError(t, sut.SetString(slot, "name", fmt.Sprintf("user %d", slot)))
	}

	for i := 0; i < cnt; i++ {
		slot, err := sut.NextAfter(types.SlotID(i - 1))
		require.NoError(t, err)

		idVal, err := sut.GetInt64(slot, "id")
		require.NoError(t, err)
		assert.EqualValues(t, int64(slot+1), idVal)

		ageVal, err := sut.GetInt8(slot, "age")
		require.NoError(t, err)
		assert.EqualValues(t, int8(slot+2), ageVal)

		nameVal, err := sut.GetString(slot, "name")
		require.NoError(t, err)
		assert.EqualValues(t, fmt.Sprintf("user %d", slot), nameVal)
	}

	trx.Unpin(sut.Block)
	require.NoError(t, trx.Commit())
}

func (ts *RecordPageTestSuite) TestDeleteSlot() {
	t := ts.T()

	sut, _, clean := ts.newTestRecordPage(t)
	defer clean()

	for i := 0; i < 20; i++ {
		slot, err := sut.InsertAfter(records.StartSlotID)
		require.NoError(t, err)
		require.EqualValues(t, i, slot)
	}

	require.NoError(t, sut.Delete(13))

	nextSlot, err := sut.NextAfter(12)
	require.NoError(t, err)
	assert.EqualValues(t, 14, nextSlot)
}

func (ts *RecordPageTestSuite) TestNoNewSlot() {
	t := ts.T()

	sut, _, clean := ts.newTestRecordPage(t)
	defer clean()

	_, err := sut.InsertAfter(types.SlotID(defaultTestBlockSize / sut.Layout.SlotSize))
	require.Error(t, err)
	assert.ErrorIs(t, err, records.ErrSlotNotFound)
}
