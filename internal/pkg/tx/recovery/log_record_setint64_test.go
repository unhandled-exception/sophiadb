package recovery_test

import (
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
)

var testSetInt64Record = recovery.NewSetInt64LogRecord(
	0x1234,
	storage.NewBlockID("testlogfile", 0x0789),
	0x0145,
	0x01020304012345fa,
)

var testRawSetInt64Record = []byte{
	0x4, 0x0, 0x0, 0x0, // op == 5
	0x34, 0x12, 0x0, 0x0, // txnum == 0x1234
	0xb, 0x0, 0x0, 0x0, // filename length == 11
	0x74, 0x65, 0x73, 0x74, 0x6c, 0x6f, 0x67, 0x66, 0x69, 0x6c, 0x65, // filename "testlogfile"
	0x89, 0x07, 0x0, 0x0, // block numer == 0x0789
	0x45, 0x01, 0x0, 0x0, // offset == 0x0145
	0xfa, 0x45, 0x23, 0x01, 0x04, 0x03, 0x02, 0x01, // value 0x01020304012345fa
}

type SetInt64LogRecordTestSuite struct {
	suite.Suite
}

func TestSetInt64LogRecordTestSuite(t *testing.T) {
	suite.Run(t, new(SetInt64LogRecordTestSuite))
}

func (ts *SetInt64LogRecordTestSuite) TestNewSetInt64LogRecord() {
	t := ts.T()

	r := recovery.NewSetInt64LogRecord(
		12345,
		storage.NewBlockID("testlogfile", 789),
		145,
		-1245,
	)
	require.NotNil(t, r)

	assert.Equal(t, "<SET_INT64, 12345, block: [file testlogfile, block 789], offset: 145, value: -1245>", r.String())
	assert.EqualValues(t, recovery.SetInt64Op, r.Op())
	assert.EqualValues(t, 12345, r.TXNum())
}

func (ts *SetInt64LogRecordTestSuite) TestNewSetInt64LogRecordFromBytes() {
	t := ts.T()

	r, err := recovery.NewSetInt64LogRecordFromBytes(testRawSetInt64Record)
	require.NoError(t, err)

	assert.Equal(t, testSetInt64Record, r)
}

func (ts *SetInt64LogRecordTestSuite) TestMarshalBytes() {
	t := ts.T()

	assert.EqualValues(t,
		testRawSetInt64Record,
		testSetInt64Record.MarshalBytes(),
	)
}

func (ts *SetInt64LogRecordTestSuite) TestUndo() {
	t := ts.T()

	mc := minimock.NewController(t)

	trxIntMock := recovery.NewTrxIntMock(mc).
		PinMock.Return(nil).
		UnpinMock.Return(nil).
		SetInt64Mock.Return(nil)

	err := testSetInt64Record.Undo(trxIntMock)
	require.NoError(t, err)
}
