package recovery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
)

type LogRecordTestSuite struct {
	suite.Suite
}

func TestLogRecordTestSuite(t *testing.T) {
	suite.Run(t, new(LogRecordTestSuite))
}

func (ts *LogRecordTestSuite) TestNewLogRecordFromBytes_OK() {
	t := ts.T()

	testCases := []struct {
		Name      string
		RawRecord []byte
		Record    interface{}
	}{
		{Name: "SetStringRecord", RawRecord: testRawSetStringRecord, Record: testSetStringRecord},
		{Name: "SetInt64Record", RawRecord: testRawSetInt64Record, Record: testSetInt64Record},
	}

	for _, tc := range testCases {
		r, err := recovery.NewLogRecordFromBytes(tc.RawRecord)
		assert.NoError(t, err, "Failed to unmarshal %s", tc.Name)

		if err == nil {
			assert.Equalf(t, tc.Record, r, "%s records isn't equals")
		}
	}
}

func (ts *LogRecordTestSuite) TestNewLogRecordFromBytes_Failed() {
	t := ts.T()

	_, err := recovery.NewLogRecordFromBytes([]byte{0xff, 0x0, 0x0, 0x0})
	assert.ErrorIs(t, err, recovery.ErrUnknownLogRecord)

	_, err = recovery.NewLogRecordFromBytes(nil)
	assert.ErrorIs(t, err, recovery.ErrEmptyLogRecord)
}
