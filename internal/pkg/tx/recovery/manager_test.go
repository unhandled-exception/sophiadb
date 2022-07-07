package recovery_test

import (
	"path/filepath"
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

type RecoveryManagerTestSuite struct {
	suite.Suite

	suiteDir string
}

const (
	testSuiteDir              = "recovery_manager_tests"
	testDataFile              = "data.dat"
	testWALFile               = "recovery_wal_log.dat"
	defaultTestBlockSize      = 400
	defaultTestBuffersPoolLen = 20
	defaultTestTxNum          = 56743
)

func TestRecoveryManagerTestsuite(t *testing.T) {
	suite.Run(t, new(RecoveryManagerTestSuite))
}

func (ts *RecoveryManagerTestSuite) SuiteDir() string {
	return ts.suiteDir
}

func (ts *RecoveryManagerTestSuite) SetupSuite() {
	ts.suiteDir = testutil.CreateSuiteTemporaryDir(ts, testSuiteDir)
}

func (ts *RecoveryManagerTestSuite) TearDownSuite() {
	testutil.RemoveSuiteTemporaryDir(ts)
}

func (ts *RecoveryManagerTestSuite) createRecoveryManager(mc minimock.Tester, txNum *types.TRX) (*recovery.Manager, *recovery.TrxIntMock, *wal.Manager, *buffers.Manager) {
	path := testutil.CreateTestTemporaryDir(ts)

	fm, err := storage.NewFileManager(path, defaultTestBlockSize)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	wal, err := wal.NewManager(fm, testWALFile)
	ts.Require().NoError(err)
	ts.Require().FileExists(filepath.Join(path, testWALFile))

	bm := buffers.NewManager(fm, wal, defaultTestBuffersPoolLen)

	trx := recovery.NewTrxIntMock(mc)

	if txNum == nil {
		newTxNum := types.TRX(defaultTestTxNum)
		txNum = &newTxNum
	}

	trx.TXNumMock.Return(*txNum).
		PinMock.Return(nil).
		UnpinMock.Return()

	rm, _ := recovery.NewManager(trx, wal, bm)

	return rm, trx, wal, bm
}

func (ts *RecoveryManagerTestSuite) fetchWAL(t *testing.T, wal *wal.Manager) []string {
	it, err := wal.Iterator()
	require.NoError(t, err)

	result := make([]string, 0)

	for it.HasNext() {
		raw, err := it.Next()
		require.NoError(t, err)

		lr, err := recovery.NewLogRecordFromBytes(raw)
		require.NoError(t, err)

		lri, _ := lr.(recovery.LogRecord)

		result = append([]string{lri.String()}, result...)
	}

	return result
}

func (ts *RecoveryManagerTestSuite) TestCommit_LogOk() {
	t := ts.T()

	mc := minimock.NewController(t)
	sut, _, wal, _ := ts.createRecoveryManager(mc, nil)

	err := sut.Commit()
	require.NoError(t, err)

	assert.Equal(t,
		[]string{
			"<START, 56743>",
			"<COMMIT, 56743>",
		},
		ts.fetchWAL(t, wal),
	)
}

func (ts *RecoveryManagerTestSuite) TestSetInt64_LogOk() {
	t := ts.T()

	mc := minimock.NewController(t)
	sut, _, wal, bm := ts.createRecoveryManager(mc, nil)

	_, _ = bm.StorageManager().Append(testDataFile)
	block, err := bm.StorageManager().Append(testDataFile)
	require.NoError(t, err)

	buf, err := bm.Pin(block)
	require.NoError(t, err)

	var (
		offset   uint32 = 25
		oldValue int64  = 49579274324325
		newValue int64  = 837509348275
	)

	buf.Content().SetInt64(offset, oldValue)

	lsn, err := sut.SetInt64(buf, offset, newValue)
	require.NoError(t, err)

	require.EqualValues(t, 2, lsn)

	// TODO: проверить восстановление

	assert.Equal(t,
		[]string{
			"<START, 56743>",
			"<SET_INT64, 56743, block: [file data.dat, block 1], offset: 25, value: 49579274324325>",
		},
		ts.fetchWAL(t, wal),
	)
}

func (ts *RecoveryManagerTestSuite) TestSetString_LogOk() {
	t := ts.T()

	mc := minimock.NewController(t)
	sut, _, wal, bm := ts.createRecoveryManager(mc, nil)

	_, _ = bm.StorageManager().Append(testDataFile)
	block, err := bm.StorageManager().Append(testDataFile)
	require.NoError(t, err)

	buf, err := bm.Pin(block)
	require.NoError(t, err)

	var (
		offset   uint32 = 25
		oldValue string = "49579274324325"
		newValue string = "837509348275"
	)

	buf.Content().SetString(offset, oldValue)

	lsn, err := sut.SetString(buf, offset, newValue)
	require.NoError(t, err)

	require.EqualValues(t, 2, lsn)

	// TODO: проверить восстановление

	assert.Equal(t,
		[]string{
			"<START, 56743>",
			"<SET_STRING, 56743, block: [file data.dat, block 1], offset: 25, value: \"49579274324325\">",
		},
		ts.fetchWAL(t, wal),
	)
}

func (ts *RecoveryManagerTestSuite) TestRollback_LogOk() {
	t := ts.T()

	mc := minimock.NewController(t)
	sut, _, wal, _ := ts.createRecoveryManager(mc, nil)

	err := sut.Rollback()
	require.NoError(t, err)

	assert.Equal(t,
		[]string{
			"<START, 56743>",
			"<ROLLBACK, 56743>",
		},
		ts.fetchWAL(t, wal),
	)
}

func (ts *RecoveryManagerTestSuite) TestRollback_RollbackDataOk() {
	t := ts.T()

	mc := minimock.NewController(t)
	sut, trx, wal, bm := ts.createRecoveryManager(mc, nil)

	block, err := bm.StorageManager().Append(testDataFile)
	require.NoError(t, err)

	buf, err := bm.Pin(block)
	require.NoError(t, err)

	tx1id := trx.TXNum()
	tx2id := types.TRX(1200)
	offset := uint32(25)
	value0 := int64(333)
	value1 := int64(1000333)
	value2 := int64(2000333)
	value3 := int64(3000333)

	trx.SetInt64Mock.Inspect(func(block *types.Block, offset uint32, value int64, okToLog bool) {
		buf.Content().SetInt64(offset, value)
	}).Return(nil)

	logRecords := []recovery.LogRecord{
		// tx1 стартанула раньше при инициализации trx
		recovery.NewStartLogRecord(tx2id),
		recovery.NewSetInt64LogRecord(tx2id, block, offset, value2),
		recovery.NewCommitLogRecord(tx2id),
		recovery.NewSetInt64LogRecord(tx1id, block, offset, value1),
		recovery.NewSetInt64LogRecord(tx1id, block, offset, value3),
	}

	for _, lr := range logRecords {
		_, _ = wal.Append(lr.MarshalBytes())
	}

	buf.Content().SetInt64(offset, value0)

	require.NoError(t, sut.Rollback())

	// tx2 реально в базе не фиксировалась, поэтому мы ожидаем,
	// что в базе будет value1 от tx1 как результат отката tx1.
	// Начальное value0 в базе быть не должно
	assert.Equal(t, value1, buf.Content().GetInt64(offset))

	log := ts.fetchWAL(t, wal)
	assert.Equal(t,
		[]string{
			"<ROLLBACK, 56743>",
		},
		log[len(log)-1:],
	)
}

func (ts *RecoveryManagerTestSuite) TestRecovery_LogOk() {
	t := ts.T()

	mc := minimock.NewController(t)
	sut, _, wal, _ := ts.createRecoveryManager(mc, nil)

	err := sut.Recover()
	require.NoError(t, err)

	// TODO: проверить восстановление

	assert.Equal(t,
		[]string{
			"<START, 56743>",
			"<CHECKPOINT>",
		},
		ts.fetchWAL(t, wal),
	)
}

func (ts *RecoveryManagerTestSuite) TestRecovery_RecoveryDataOk() {
	t := ts.T()

	mc := minimock.NewController(t)
	sut, trx, wal, bm := ts.createRecoveryManager(mc, nil)

	block, err := bm.StorageManager().Append(testDataFile)
	require.NoError(t, err)

	buf, err := bm.Pin(block)
	require.NoError(t, err)

	tx1id := trx.TXNum()
	tx2id := types.TRX(1200)
	tx3id := types.TRX(2200)
	tx4id := types.TRX(3200)
	offset := uint32(25)
	value0 := int64(333)
	value1 := int64(1000333)
	value2 := int64(2000333)
	value3 := int64(3000333)
	value4 := int64(3000333)
	value5 := int64(3000333)

	trx.SetInt64Mock.Inspect(func(block *types.Block, offset uint32, value int64, okToLog bool) {
		buf.Content().SetInt64(offset, value)
	}).Return(nil)

	logRecords := []recovery.LogRecord{
		// tx1 стартанула раньше при инициализации trx
		recovery.NewStartLogRecord(tx4id),
		recovery.NewSetInt64LogRecord(tx4id, block, offset, value4),

		recovery.NewCheckpointLogRecord(),

		recovery.NewStartLogRecord(tx2id),
		recovery.NewSetInt64LogRecord(tx2id, block, offset, value2),
		recovery.NewCommitLogRecord(tx2id),

		recovery.NewSetInt64LogRecord(tx1id, block, offset, value5),

		recovery.NewStartLogRecord(tx3id),
		recovery.NewSetInt64LogRecord(tx3id, block, offset, value3),
		recovery.NewRollbackLogRecord(tx3id),

		recovery.NewSetInt64LogRecord(tx1id, block, offset, value1),
	}

	for _, lr := range logRecords {
		_, _ = wal.Append(lr.MarshalBytes())
	}

	buf.Content().SetInt64(offset, value0)

	require.NoError(t, sut.Recover())

	// Должно вернуться значение value5 из tx1
	assert.Equal(t, value5, buf.Content().GetInt64(offset))

	log := ts.fetchWAL(t, wal)
	assert.Equal(t,
		[]string{
			"<CHECKPOINT>",
		},
		log[len(log)-1:],
	)
}
