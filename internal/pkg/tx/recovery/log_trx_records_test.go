package recovery_test

import (
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
)

var (
	testStartLogRecord    = recovery.NewStartLogRecord(0x1234)
	testRawStartLogRecord = []byte{
		0x1, 0x0, 0x0, 0x0, // op == 1
		0x34, 0x12, 0x0, 0x0, // txnum == 0x1234
	}

	testCommitLogRecord    = recovery.NewCommitLogRecord(0x1234)
	testRawCommitLogRecord = []byte{
		0x2, 0x0, 0x0, 0x0, // op == 1
		0x34, 0x12, 0x0, 0x0, // txnum == 0x1234
	}

	testRollbackLogRecord    = recovery.NewRollbackLogRecord(0x1234)
	testRawRollbackLogRecord = []byte{
		0x3, 0x0, 0x0, 0x0, // op == 1
		0x34, 0x12, 0x0, 0x0, // txnum == 0x1234
	}
)

type TrxLogRecordsTestSuite struct {
	suite.Suite
}

func TestTrxLogRecordsTestSuite(t *testing.T) {
	suite.Run(t, new(TrxLogRecordsTestSuite))
}

func (ts *TrxLogRecordsTestSuite) TestStartLogRecord_NewStartLogRecord() {
	t := ts.T()

	r := recovery.NewStartLogRecord(12345)
	require.NotNil(t, r)

	assert.Equal(t, "<START, 12345>", r.String())
	assert.EqualValues(t, recovery.StartOp, r.Op())
	assert.EqualValues(t, 12345, r.TXNum())
}

func (ts *TrxLogRecordsTestSuite) TestStartLogRecord_NewStartLogRecordFromBytes() {
	t := ts.T()

	r, err := recovery.NewStartLogRecordFromBytes(testRawStartLogRecord)
	require.NoError(t, err)

	assert.Equal(t, testStartLogRecord, r)
}

func (ts *TrxLogRecordsTestSuite) TestStartLogRecord_MarshalBytes() {
	t := ts.T()

	assert.EqualValues(t,
		testRawStartLogRecord,
		testStartLogRecord.MarshalBytes(),
	)
}

func (ts *TrxLogRecordsTestSuite) TestStartLogRecord_Undo() {
	t := ts.T()

	mc := minimock.NewController(t)
	trxIntMock := recovery.NewTrxIntMock(mc)

	err := testStartLogRecord.Undo(trxIntMock)
	require.NoError(t, err)
}

func (ts *TrxLogRecordsTestSuite) TestCommitLogRecord_NewCommitLogRecord() {
	t := ts.T()

	r := recovery.NewCommitLogRecord(12345)
	require.NotNil(t, r)

	assert.Equal(t, "<COMMIT, 12345>", r.String())
	assert.EqualValues(t, recovery.CommitOp, r.Op())
	assert.EqualValues(t, 12345, r.TXNum())
}

func (ts *TrxLogRecordsTestSuite) TestCommitLogRecord_NewCommitLogRecordFromBytes() {
	t := ts.T()

	r, err := recovery.NewCommitLogRecordFromBytes(testRawCommitLogRecord)
	require.NoError(t, err)

	assert.Equal(t, testCommitLogRecord, r)
}

func (ts *TrxLogRecordsTestSuite) TestCommitLogRecord_MarshalBytes() {
	t := ts.T()

	assert.EqualValues(t,
		testRawCommitLogRecord,
		testCommitLogRecord.MarshalBytes(),
	)
}

func (ts *TrxLogRecordsTestSuite) TestCommitLogRecord_Undo() {
	t := ts.T()

	mc := minimock.NewController(t)
	trxIntMock := recovery.NewTrxIntMock(mc)

	err := testCommitLogRecord.Undo(trxIntMock)
	require.NoError(t, err)
}

func (ts *TrxLogRecordsTestSuite) TestRollbackLogRecord_NewRollbackLogRecord() {
	t := ts.T()

	r := recovery.NewRollbackLogRecord(12345)
	require.NotNil(t, r)

	assert.Equal(t, "<ROLLBACK, 12345>", r.String())
	assert.EqualValues(t, recovery.RollbackOp, r.Op())
	assert.EqualValues(t, 12345, r.TXNum())
}

func (ts *TrxLogRecordsTestSuite) TestRollbackLogRecord_NewRollbackLogRecordFromBytes() {
	t := ts.T()

	r, err := recovery.NewRollbackLogRecordFromBytes(testRawRollbackLogRecord)
	require.NoError(t, err)

	assert.Equal(t, testRollbackLogRecord, r)
}

func (ts *TrxLogRecordsTestSuite) TestRollbackLogRecord_MarshalBytes() {
	t := ts.T()

	assert.EqualValues(t,
		testRawRollbackLogRecord,
		testRollbackLogRecord.MarshalBytes(),
	)
}

func (ts *TrxLogRecordsTestSuite) TestRollbackLogRecord_Undo() {
	t := ts.T()

	mc := minimock.NewController(t)
	trxIntMock := recovery.NewTrxIntMock(mc)

	err := testRollbackLogRecord.Undo(trxIntMock)
	require.NoError(t, err)
}
