package recovery_test

import (
	"testing"

	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/tx/recovery"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

var testSetInt8LogRecord = recovery.NewSetInt8LogRecord(
	0x1234,
	types.Block{Filename: "testlogfile", Number: 0x0789},
	0x0145,
	-6,
)

var testRawSetInt8LogRecord = []byte{
	0x6, 0x0, 0x0, 0x0, // op == 4
	0x34, 0x12, 0x0, 0x0, // txnum == 0x1234
	0xb, 0x0, 0x0, 0x0, // filename length == 11
	0x74, 0x65, 0x73, 0x74, 0x6c, 0x6f, 0x67, 0x66, 0x69, 0x6c, 0x65, // filename "testlogfile"
	0x89, 0x07, 0x0, 0x0, // block numer == 0x0789
	0x45, 0x01, 0x0, 0x0, // offset == 0x0145
	0xfa, // value 0xfa
}

type SetInt8LogRecordTestSuite struct {
	suite.Suite
}

func TestSetInt8LogRecordTestSuite(t *testing.T) {
	suite.Run(t, new(SetInt8LogRecordTestSuite))
}

func (ts *SetInt8LogRecordTestSuite) TestNewSetInt8LogRecord() {
	t := ts.T()

	r := recovery.NewSetInt8LogRecord(
		12345,
		types.Block{Filename: "testlogfile", Number: 789},
		145,
		-125,
	)
	require.NotNil(t, r)

	assert.Equal(t, "<SET_INT8, 12345, block: [file testlogfile, block 789], offset: 145, value: -125>", r.String())
	assert.EqualValues(t, recovery.SetInt8Op, r.Op())
	assert.EqualValues(t, 12345, r.TXNum())
}

func (ts *SetInt8LogRecordTestSuite) TestNewSetInt8LogRecordFromBytes() {
	t := ts.T()

	r, err := recovery.NewSetInt8LogRecordFromBytes(testRawSetInt8LogRecord)
	require.NoError(t, err)

	assert.Equal(t, testSetInt8LogRecord, r)
}

func (ts *SetInt8LogRecordTestSuite) TestMarshalBytes() {
	t := ts.T()

	assert.EqualValues(t,
		testRawSetInt8LogRecord,
		testSetInt8LogRecord.MarshalBytes(),
	)
}

func (ts *SetInt8LogRecordTestSuite) TestUndo() {
	t := ts.T()

	mc := minimock.NewController(t)

	trxIntMock := recovery.NewTrxIntMock(mc).
		PinMock.Return(nil).
		UnpinMock.Return().
		SetInt8Mock.Return(nil)

	err := testSetInt8LogRecord.Undo(trxIntMock)
	require.NoError(t, err)
}
