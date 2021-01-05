package storage

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type PageTestSuite struct {
	suite.Suite
}

func TestPageTestSuite(t *testing.T) {
	suite.Run(t, new(PageTestSuite))
}

func (ts PageTestSuite) TestCreatePage() {
	p := NewPage(400)
	ts.Assert().Equal(400, p.Len())
	for i, b := range p.Content() {
		ts.Assert().Equal(uint8(0x00), b, "Byte %d equals to %x", i, b)
	}
}

func (ts PageTestSuite) TestPutAndFetchBytes() {
	p := NewPage(20)
	p.putBytes(0, []byte{0x13, 0x14, 0x00, 0x15})
	p.putBytes(8, []byte{0x23, 0x24, 0x00, 0x25})

	ts.Assert().Equal(
		[]byte{0x13, 0x14, 0x00, 0x15, 0x00, 0x00, 0x00, 0x00},
		p.fetchBytes(0, 8),
	)
	ts.Assert().Equal(
		[]byte{0x23, 0x24, 0x00, 0x25, 0x00, 0x00, 0x00, 0x00},
		p.fetchBytes(8, 8),
	)
}

func (ts PageTestSuite) TestGetAndSetInt32() {
	p := NewPage(24)
	p.SetInt32(0, 12345)
	p.SetInt32(4, -12345)
	p.SetInt32(16, 0x7fffffff)
	ts.Assert().Equal(int32(12345), p.GetInt32(0))
	ts.Assert().Equal(int32(-12345), p.GetInt32(4))
	ts.Assert().Equal(int32(0x7fffffff), p.GetInt32(16))
}

func (ts PageTestSuite) TestGetAndSetInt64() {
	p := NewPage(24)
	p.SetInt64(0, 12345)
	p.SetInt64(8, -12345)
	p.SetInt64(16, 0x7fffffffffffffff)
	ts.Assert().Equal(int64(12345), p.GetInt64(0))
	ts.Assert().Equal(int64(-12345), p.GetInt64(8))
	ts.Assert().Equal(int64(0x7fffffffffffffff), p.GetInt64(16))
}

func (ts PageTestSuite) TestGetAndSetString() {
	p := NewPage(200)
	cases := []string{
		"Тестовая string 1",
		"Еще одна тестовая string",
		"И снова тестовая строка",
	}
	var offset int = 0
	for _, s := range cases {
		p.SetString(offset, s)
		offset += len(s) + 4
	}

	offset = 0
	for _, s := range cases {
		ts.Assert().Equal(s, p.GetString(offset))
		offset += len(s) + 4
	}
}

func (ts PageTestSuite) TestGetAndSetFloat32() {
	p := NewPage(24)
	p.SetFloat32(0, 12345.245)
	p.SetFloat32(4, -12345.245)
	p.SetFloat32(16, 0x7fffffff)
	ts.Assert().Equal(float32(12345.245), p.GetFloat32(0))
	ts.Assert().Equal(float32(-12345.245), p.GetFloat32(4))
	ts.Assert().Equal(float32(0x7fffffff), p.GetFloat32(16))
}

func (ts PageTestSuite) TestGetAndSetBool() {
	p := NewPage(4)
	p.SetBool(0, true)
	p.SetBool(1, false)
	p.SetBool(2, true)
	ts.Assert().Equal(true, p.GetBool(0))
	ts.Assert().Equal(false, p.GetBool(1))
	ts.Assert().Equal(true, p.GetBool(2))
	ts.Assert().Equal([]byte{boolTrueMark, boolFalseMark, boolTrueMark, 0x0}, p.Content())
}
