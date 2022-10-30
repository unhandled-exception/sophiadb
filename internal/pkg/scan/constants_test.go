package scan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

var (
	_ scan.Constant = scan.Int64Constant{}
	_ scan.Constant = scan.Int8Constant{}
	_ scan.Constant = scan.StringConstant{}
)

type ConstantsTestSuite struct {
	suite.Suite
}

func TestConstantTestSute(t *testing.T) {
	suite.Run(t, new(ConstantsTestSuite))
}

func (ts *ConstantsTestSuite) TestInt64Constant() {
	t := ts.T()

	var value int64 = 12345
	sut := scan.NewInt64Constant(value)

	res, ok := sut.Value().(int64)
	assert.True(t, ok)
	assert.Equal(t, value, res)
	assert.Equal(t, "12345", sut.String())

	assert.Equal(t, scan.CompUncomparable, sut.CompareTo(scan.NewStringConstant("")))

	assert.Equal(t, scan.CompEqual, sut.CompareTo(scan.NewInt64Constant(value)))
	assert.Equal(t, scan.CompLess, sut.CompareTo(scan.NewInt64Constant(value+1)))
	assert.Equal(t, scan.CompGreat, sut.CompareTo(scan.NewInt64Constant(value-1)))

	var smallValue int8 = 123
	sut2 := scan.NewInt64Constant(int64(smallValue))

	assert.Equal(t, scan.CompEqual, sut2.CompareTo(scan.NewInt8Constant(smallValue)))
	assert.Equal(t, scan.CompLess, sut2.CompareTo(scan.NewInt8Constant(smallValue+1)))
	assert.Equal(t, scan.CompGreat, sut2.CompareTo(scan.NewInt8Constant(smallValue-1)))
}

func (ts *ConstantsTestSuite) TestInt8Constant() {
	t := ts.T()

	var value int8 = -123
	sut := scan.NewInt8Constant(value)

	res, ok := sut.Value().(int8)
	assert.True(t, ok)
	assert.Equal(t, value, res)
	assert.Equal(t, "-123", sut.String())

	assert.Equal(t, scan.CompUncomparable, sut.CompareTo(scan.NewStringConstant("")))

	assert.Equal(t, scan.CompEqual, sut.CompareTo(scan.NewInt8Constant(value)))
	assert.Equal(t, scan.CompLess, sut.CompareTo(scan.NewInt8Constant(value+1)))
	assert.Equal(t, scan.CompGreat, sut.CompareTo(scan.NewInt8Constant(value-1)))

	var bigValue int64 = 123
	sut2 := scan.NewInt8Constant(int8(bigValue))

	assert.Equal(t, scan.CompEqual, sut2.CompareTo(scan.NewInt64Constant(bigValue)))
	assert.Equal(t, scan.CompLess, sut2.CompareTo(scan.NewInt64Constant(bigValue+1)))
	assert.Equal(t, scan.CompGreat, sut2.CompareTo(scan.NewInt64Constant(bigValue-1)))
}

func (ts *ConstantsTestSuite) TestStringConstant() {
	t := ts.T()

	var value string = "test"
	sut := scan.NewStringConstant(value)

	res, ok := sut.Value().(string)
	assert.True(t, ok)
	assert.Equal(t, value, res)
	assert.Equal(t, `'`+value+`'`, sut.String())

	assert.Equal(t, scan.CompEqual, sut.CompareTo(scan.NewStringConstant(value)))
	assert.Equal(t, scan.CompLess, sut.CompareTo(scan.NewStringConstant(value+"+")))
	assert.Equal(t, scan.CompGreat, sut.CompareTo(scan.NewStringConstant(value[:len(value)-1])))

	assert.Equal(t, scan.CompUncomparable, sut.CompareTo(scan.NewInt64Constant(0)))
}
