package types_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/types"
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
	sut := types.NewInt64Constant(value)

	res, ok := sut.Value().(int64)
	assert.True(t, ok)
	assert.Equal(t, value, res)
}

func (ts *ConstantsTestSuite) TestInt8Constant() {
	t := ts.T()

	var value int8 = -123
	sut := types.NewInt8Constant(value)

	res, ok := sut.Value().(int8)
	assert.True(t, ok)
	assert.Equal(t, value, res)
}

func (ts *ConstantsTestSuite) TestStringConstant() {
	t := ts.T()

	var value string = "test"
	sut := types.NewStringConstant(value)

	res, ok := sut.Value().(string)
	assert.True(t, ok)
	assert.Equal(t, value, res)
}
