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

func (ts PageTestSuite) TestGetAndSetInt() {
	p := NewPage(20)
	p.SetInt(0, 12345)
	p.SetInt(8, -12345)
	ts.Assert().Equal(int64(12345), p.GetInt(0))
	ts.Assert().Equal(int64(-12345), p.GetInt(8))
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
		offset += len(s) + 8
	}

	offset = 0
	for _, s := range cases {
		ts.Assert().Equal(s, p.GetString(offset))
		offset += len(s) + 8
	}
}
