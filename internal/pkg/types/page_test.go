package types_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type PageTestSuite struct {
	suite.Suite
}

func TestPageTestSuite(t *testing.T) {
	suite.Run(t, new(PageTestSuite))
}

func (ts *PageTestSuite) TestNewPage() {
	p := types.NewPage(400)
	ts.Len(p.Content(), 400)

	for i, b := range p.Content() {
		ts.Equal(uint8(0x00), b, "Byte %d equals to %x", i, b)
	}
}

func (ts *PageTestSuite) TestNewPageFromBytes() {
	t := ts.T()

	order := binary.LittleEndian
	raw := make([]byte, 8)
	order.PutUint64(raw, uint64(12345678))

	p := types.NewPageFromBytes(raw)
	require.EqualValues(t, 8, p.Len())

	assert.EqualValues(t, 12345678, p.GetInt64(0))
}

func (ts *PageTestSuite) TestPutAndFetchBytes() {
	p := types.NewPage(20)
	p.PutBytes(0, []byte{0x13, 0x14, 0x00, 0x15})
	p.PutBytes(8, []byte{0x23, 0x24, 0x00, 0x25})

	ts.Equal(
		[]byte{0x13, 0x14, 0x00, 0x15, 0x00, 0x00, 0x00, 0x00},
		p.FetchBytes(0, 8),
	)
	ts.Equal(
		[]byte{0x23, 0x24, 0x00, 0x25, 0x00, 0x00, 0x00, 0x00},
		p.FetchBytes(8, 8),
	)
}

func (ts *PageTestSuite) TestGetAndSetInt32() {
	p := types.NewPage(24)
	p.SetInt32(0, 12345)
	p.SetInt32(4, -12345)
	p.SetInt32(16, 0x7fffffff)
	ts.Equal(int32(12345), p.GetInt32(0))
	ts.Equal(int32(-12345), p.GetInt32(4))
	ts.Equal(int32(0x7fffffff), p.GetInt32(16))
}

func (ts *PageTestSuite) TestGetAndSetUint32() {
	p := types.NewPage(24)
	p.SetUint32(0, 12345)
	p.SetUint32(16, 0xffffffff)
	ts.Equal(uint32(12345), p.GetUint32(0))
	ts.Equal(uint32(0xffffffff), p.GetUint32(16))
}

func (ts *PageTestSuite) TestGetAndSetInt64() {
	p := types.NewPage(24)
	p.SetInt64(0, 12345)
	p.SetInt64(8, -12345)
	p.SetInt64(16, 0x7fffffffffffffff)
	ts.Equal(int64(12345), p.GetInt64(0))
	ts.Equal(int64(-12345), p.GetInt64(8))
	ts.Equal(int64(0x7fffffffffffffff), p.GetInt64(16))
}

func (ts *PageTestSuite) TestGetAndSetString() {
	p := types.NewPage(200)
	cases := []string{
		"Тестовая string 1",
		"Еще одна тестовая string",
		"И снова тестовая строка",
	}

	var offset uint32

	for _, s := range cases {
		p.SetString(offset, s)
		offset += uint32(len(s) + 4)
	}

	offset = 0
	for _, s := range cases {
		ts.Equal(s, p.GetString(offset))
		offset += uint32(len(s) + 4)
	}
}

func (ts *PageTestSuite) TestGetAndSetFloat32() {
	p := types.NewPage(24)
	p.SetFloat32(0, 12345.245)
	p.SetFloat32(4, -12345.245)
	p.SetFloat32(16, 0x7fffffff)
	ts.Equal(float32(12345.245), p.GetFloat32(0))
	ts.Equal(float32(-12345.245), p.GetFloat32(4))
	ts.Equal(float32(0x7fffffff), p.GetFloat32(16))
}

func (ts *PageTestSuite) TestGetAndSetBool() {
	p := types.NewPage(4)
	p.SetBool(0, true)
	p.SetBool(1, false)
	p.SetBool(2, true)
	ts.Equal(true, p.GetBool(0))
	ts.Equal(false, p.GetBool(1))
	ts.Equal(true, p.GetBool(2))
	ts.Equal([]byte{types.BoolTrueMark, types.BoolFalseMark, types.BoolTrueMark, 0x0}, p.Content())
}
