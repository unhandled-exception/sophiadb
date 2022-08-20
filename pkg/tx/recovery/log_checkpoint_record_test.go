package recovery_test

import (
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/tx/recovery"
)

var (
	testCheckpointLogRecord    = recovery.NewCheckpointLogRecord()
	testRawCheckpointLogRecord = []byte{
		0xff, 0x0, 0x0, 0x0, // op == 0
	}
)

type CheckpointLogRecordsTestSuite struct {
	suite.Suite
}

func TestCheckpointLogRecordsTestSuite(t *testing.T) {
	suite.Run(t, new(CheckpointLogRecordsTestSuite))
}

func (ts *CheckpointLogRecordsTestSuite) TestCheckpointLogRecord_NewCheckpointLogRecord() {
	t := ts.T()

	r := recovery.NewCheckpointLogRecord()
	require.NotNil(t, r)

	assert.Equal(t, "<CHECKPOINT>", r.String())
	assert.EqualValues(t, recovery.CheckpointOp, r.Op())
}

func (ts *CheckpointLogRecordsTestSuite) TestCheckpointLogRecord_NewCheckpointLogRecordFromBytes() {
	t := ts.T()

	r, err := recovery.NewCheckpointLogRecordFromBytes(testRawCheckpointLogRecord)
	require.NoError(t, err)

	assert.Equal(t, testCheckpointLogRecord, r)
}

func (ts *CheckpointLogRecordsTestSuite) TestCheckpointLogRecord_MarshalBytes() {
	t := ts.T()

	assert.EqualValues(t,
		testRawCheckpointLogRecord,
		testCheckpointLogRecord.MarshalBytes(),
	)
}

func (ts *CheckpointLogRecordsTestSuite) TestCheckpointLogRecord_Undo() {
	t := ts.T()

	mc := minimock.NewController(t)
	trxIntMock := recovery.NewTrxIntMock(mc)

	err := testCheckpointLogRecord.Undo(trxIntMock)
	require.NoError(t, err)
}
