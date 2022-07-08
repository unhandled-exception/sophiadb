package transaction_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/buffers"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/testutil"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
	"github.com/unhandled-exception/sophiadb/internal/pkg/wal"
)

const (
	testDataFile              = "data.dat"
	testWALFile               = "transactions_wal.dat"
	testLastTRX               = 1000
	defaultTestBlockSize      = 400
	defaultTestBuffersPoolLen = 100
	defaultLockTimeout        = 100 * time.Millisecond
)

type TransactionTestSuite struct {
	testutil.Suite
}

func TestTransactionTestSuite(t *testing.T) {
	suite.Run(t, new(TransactionTestSuite))
}

func (ts *TransactionTestSuite) newTRXManager(lockTimeout time.Duration) (*transaction.TRXManager, *storage.Manager) {
	path := ts.CreateTestTemporaryDir()

	fm, err := storage.NewFileManager(path, defaultTestBlockSize)
	ts.Require().NoError(err)
	ts.Require().NotNil(fm)

	lm, err := wal.NewManager(fm, testWALFile)
	ts.Require().NoError(err)
	ts.Require().FileExists(filepath.Join(path, testWALFile))

	bm := buffers.NewManager(fm, lm, defaultTestBuffersPoolLen)

	m := transaction.NewTRXManager(fm, bm, lm, transaction.WithLockTimeout(lockTimeout))
	m.SetLastTRX(testLastTRX)

	return m, fm
}

func (ts *TransactionTestSuite) fetchWAL(t *testing.T, trxMan *transaction.TRXManager) []string {
	it, err := trxMan.LogManager().Iterator()
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

func (ts *TransactionTestSuite) TestSequenceCase() {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout)
	defer fm.Close()

	iVal := int64(80)
	iOffset := uint32(80)

	sVal := "first string"
	sOffset := uint32(40)

	// Задаём начальные значения на диске. Не пишем в wal
	tx1, err := trxMan.Transaction()
	require.NoError(t, err)

	block1, err := tx1.Append(testDataFile)
	require.NoError(t, err)

	size, err := tx1.Size(testDataFile)
	require.NoError(t, err)
	assert.EqualValues(t, 1, size)

	require.NoError(t, tx1.Pin(block1))
	require.NoError(t, tx1.SetInt64(block1, iOffset, iVal, false))
	require.NoError(t, tx1.SetString(block1, sOffset, sVal, false))
	require.NoError(t, tx1.Commit())

	// Тестируем новую удачную транзакцию с записью в лог
	tx2, err := trxMan.Transaction()
	require.NoError(t, err)
	require.NoError(t, tx2.Pin(block1))

	iv, err := tx2.GetInt64(block1, iOffset)
	require.NoError(t, err)
	require.Equal(t, iv, iVal)

	sv, err := tx2.GetString(block1, sOffset)
	require.NoError(t, err)
	require.Equal(t, sv, sVal)

	iVal = iv + 1
	sVal = sv + " suffix"

	require.NoError(t, tx2.SetInt64(block1, iOffset, iVal, true))
	require.NoError(t, tx2.SetString(block1, sOffset, sVal, true))
	require.NoError(t, tx2.Commit())

	// Тестируем новую транзакцию с откатом записью в лог
	tx3, err := trxMan.Transaction()
	require.NoError(t, err)
	require.NoError(t, tx3.Pin(block1))

	iv, err = tx3.GetInt64(block1, iOffset)
	require.NoError(t, err)
	require.Equal(t, iv, iVal)

	sv, err = tx3.GetString(block1, sOffset)
	require.NoError(t, err)
	require.Equal(t, sv, sVal)

	require.NoError(t, tx3.SetInt64(block1, iOffset, iVal+1, true))
	require.NoError(t, tx3.SetString(block1, sOffset, sVal+" rb", true))
	require.NoError(t, tx3.Rollback())

	// Проверяем что после отката на странице
	tx4, err := trxMan.Transaction()
	require.NoError(t, err)
	require.NoError(t, tx4.Pin(block1))

	iv, err = tx4.GetInt64(block1, iOffset)
	require.NoError(t, err)
	require.Equal(t, iv, iVal)

	sv, err = tx4.GetString(block1, sOffset)
	require.NoError(t, err)
	require.Equal(t, sv, sVal)

	require.NoError(t, tx4.Commit())

	// Проверяем, что очистили буферы
	assert.EqualValues(t, defaultTestBuffersPoolLen, tx4.AvailableBuffersCount())
	assert.EqualValues(t, defaultTestBlockSize, tx4.BlockSize())

	// Проверяем WAL
	assert.Equal(t, ts.fetchWAL(t, trxMan),
		[]string{
			"<START, 1001>",
			"<COMMIT, 1001>",
			"<START, 1002>",
			"<SET_INT64, 1002, block: [file data.dat, block 0], offset: 80, value: 80>",
			"<SET_STRING, 1002, block: [file data.dat, block 0], offset: 40, value: \"first string\">",
			"<COMMIT, 1002>",
			"<START, 1003>",
			"<SET_INT64, 1003, block: [file data.dat, block 0], offset: 80, value: 81>",
			"<SET_STRING, 1003, block: [file data.dat, block 0], offset: 40, value: \"first string suffix\">",
			"<ROLLBACK, 1003>",
			"<START, 1004>",
			"<COMMIT, 1004>",
		},
	)

	// Проверяем что на диск записано
	page := types.NewPage(defaultTestBlockSize)
	require.NoError(t, fm.Read(block1, page))

	assert.EqualValues(t, iVal, page.GetInt64(iOffset))
	assert.EqualValues(t, sVal, page.GetString(sOffset))
}

func (ts *TransactionTestSuite) TestRecovery() {
	t := ts.T()

	trxMan, fm := ts.newTRXManager(defaultLockTimeout)
	defer fm.Close()

	wal := trxMan.LogManager()

	iOffset := uint32(80)
	sOffset := uint32(40)

	block1 := types.NewBlock(testDataFile, 0)

	trxIDS := []types.TRX{
		trxMan.TRXGen().NextTRX(),
		trxMan.TRXGen().NextTRX(),
		trxMan.TRXGen().NextTRX(),
		trxMan.TRXGen().NextTRX(),
	}

	logRecords := []recovery.LogRecord{
		recovery.NewStartLogRecord(trxIDS[0]),
		recovery.NewSetInt64LogRecord(trxIDS[0], block1, iOffset, -345),
		recovery.NewSetStringLogRecord(trxIDS[0], block1, sOffset, "invisible string 0"),
		recovery.NewCheckpointLogRecord(),

		recovery.NewStartLogRecord(trxIDS[3]),
		recovery.NewSetInt64LogRecord(trxIDS[3], block1, iOffset, -3345),
		recovery.NewSetStringLogRecord(trxIDS[3], block1, sOffset, "invisible string 3"),

		recovery.NewStartLogRecord(trxIDS[1]),
		recovery.NewSetInt64LogRecord(trxIDS[1], block1, iOffset, -1345),
		recovery.NewSetStringLogRecord(trxIDS[1], block1, sOffset, "invisible string 1"),
		recovery.NewCommitLogRecord(trxIDS[1]),

		recovery.NewStartLogRecord(trxIDS[2]),
		recovery.NewSetInt64LogRecord(trxIDS[2], block1, iOffset, -2345),
		recovery.NewSetStringLogRecord(trxIDS[2], block1, sOffset, "invisible string 2"),
		recovery.NewRollbackLogRecord(trxIDS[2]),

		recovery.NewSetStringLogRecord(trxIDS[3], block1, sOffset, "invisible string 10"),
		recovery.NewSetInt64LogRecord(trxIDS[3], block1, iOffset, -10345),
	}

	for _, rec := range logRecords {
		_, err := wal.Append(rec.MarshalBytes())
		require.NoError(t, err)
	}

	sut, err := trxMan.Transaction()
	require.NoError(t, err)

	_, err = sut.Append(testDataFile)
	require.NoError(t, err)

	// Восстанавливаем
	require.NoError(t, sut.Recover())

	// Проверяем что на диск записано
	page := types.NewPage(defaultTestBlockSize)
	require.NoError(t, fm.Read(block1, page))

	assert.EqualValues(t, -3345, page.GetInt64(iOffset))
	assert.EqualValues(t, "invisible string 3", page.GetString(sOffset))

	// Проверяем, что в WAL не попали лишние записи
	assert.Equal(t, ts.fetchWAL(t, trxMan)[len(logRecords):],
		[]string{
			"<START, 1005>",
			"<CHECKPOINT>",
		},
	)
}

func (ts *TransactionTestSuite) TestConcurrentCase() {
	t := ts.T()

	testTRXCount := int64(10)

	trxMan, fm := ts.newTRXManager(10000 * time.Millisecond)
	defer fm.Close()

	block1, err := fm.Append(testDataFile)
	require.NoError(t, err)

	iVal := int64(80)
	iOffset := uint32(80)

	sVal := "first string"
	sOffset := uint32(40)

	next := make(chan int64, 1)

	wg := sync.WaitGroup{}
	wg.Add(int(testTRXCount))

	for i := int64(1); i <= testTRXCount; i++ {
		go func() {
			defer wg.Done()

			var err error
			defer func() {
				assert.NoError(t, err)
			}()

			// Ждём старта одной и транзакций
			num := <-next

			// Запускаем только одну пишущую транзакцию, чтобы проверить, что она дождётся когда отвалятся локи в читающих транзакциях
			trxType := "read"
			if num < 2 {
				trxType = "write"
			}

			sut, err := trxMan.Transaction()
			if err != nil {
				return
			}

			t.Logf("start %s trx %d (%d)", trxType, sut.TXNum(), num)

			if err := sut.Pin(block1); err != nil {
				return
			}

			next <- (num + 1)

			time.Sleep(5 * time.Millisecond)

			if _, err = sut.GetInt64(block1, iOffset); err != nil {
				return
			}

			if _, err = sut.GetString(block1, sOffset); err != nil {
				return
			}

			if trxType == "write" {
				if err = sut.SetInt64(block1, iOffset, iVal+num, true); err != nil {
					return
				}

				if err = sut.SetString(block1, sOffset, fmt.Sprintf("%s %d", sVal, num), true); err != nil {
					return
				}
			}

			if err = sut.Commit(); err != nil {
				return
			}

			t.Logf("commit %s trx %d (%d)", trxType, sut.TXNum(), num)
		}()
	}

	next <- 1

	wg.Wait()

	// Проверяем что на диск записано
	page := types.NewPage(defaultTestBlockSize)
	require.NoError(t, fm.Read(block1, page))

	assert.EqualValues(t, iVal+1, page.GetInt64(iOffset))
	assert.EqualValues(t, sVal+" 1", page.GetString(sOffset))
}
