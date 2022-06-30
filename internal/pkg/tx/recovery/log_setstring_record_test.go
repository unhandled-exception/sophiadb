package recovery_test

import (
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

var testSetStringLogRecord = recovery.NewSetStringLogRecord(
	0x1234,
	types.NewBlockID("testlogfile", 0x0789),
	0x0145,
	"Test string value",
)

var testRawSetStringLogRecord = []byte{
	0x5, 0x0, 0x0, 0x0, // op == 5
	0x34, 0x12, 0x0, 0x0, // txnum == 0x1234
	0xb, 0x0, 0x0, 0x0, // filename length == 11
	0x74, 0x65, 0x73, 0x74, 0x6c, 0x6f, 0x67, 0x66, 0x69, 0x6c, 0x65, // filename "testlogfile"
	0x89, 0x07, 0x0, 0x0, // block numer == 0x0789
	0x45, 0x01, 0x0, 0x0, // offset == 0x0145
	0x11, 0x0, 0x0, 0x0, // value len = 17
	0x54, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x20, 0x76, 0x61, 0x6c, 0x75, 0x65, // value "Test string value"
}

type SetStringLogRecordTestSuite struct {
	suite.Suite
}

func TestSetStringLogRecordTestSuite(t *testing.T) {
	suite.Run(t, new(SetStringLogRecordTestSuite))
}

func (ts *SetStringLogRecordTestSuite) TestNewSetStringLogRecord() {
	t := ts.T()

	r := recovery.NewSetStringLogRecord(
		12345,
		types.NewBlockID("testlogfile", 789),
		145,
		"Test string value",
	)
	require.NotNil(t, r)

	assert.Equal(t, "<SET_STRING, 12345, block: [file testlogfile, block 789], offset: 145, value: \"Test string value\">", r.String())
	assert.EqualValues(t, recovery.SetStringOp, r.Op())
	assert.EqualValues(t, 12345, r.TXNum())
}

func (ts *SetStringLogRecordTestSuite) TestNewSetStringLogRecordFromBytes() {
	t := ts.T()

	r, err := recovery.NewSetStringLogRecordFromBytes(testRawSetStringLogRecord)
	require.NoError(t, err)

	assert.Equal(t, testSetStringLogRecord, r)
}

func (ts *SetStringLogRecordTestSuite) TestMarshalBytes() {
	t := ts.T()

	assert.EqualValues(t,
		testRawSetStringLogRecord,
		testSetStringLogRecord.MarshalBytes(),
	)
}

func (ts *SetStringLogRecordTestSuite) TestUndo() {
	t := ts.T()

	mc := minimock.NewController(t)

	trxIntMock := recovery.NewTrxIntMock(mc).
		PinMock.Return(nil).
		UnpinMock.Return(nil).
		SetStringMock.Return(nil)

	err := testSetStringLogRecord.Undo(trxIntMock)
	require.NoError(t, err)
}
