package types_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type BlockTestSuite struct {
	suite.Suite
}

func TestBlockTestSuite(t *testing.T) {
	suite.Run(t, new(BlockTestSuite))
}

func (ts *BlockTestSuite) TestCreateBlock() {
	filename := "block_filename"
	blkNum := int32(12345)
	block := types.NewBlock(filename, blkNum)
	ts.Equal(filename, block.Filename)
	ts.Equal(blkNum, block.Number)
	ts.Equal("[file block_filename, block 12345]", block.String())
	ts.Equal("[block_filename][12345]", block.HashKey())
}

func (ts *BlockTestSuite) TestBlockEquals() {
	block1 := types.NewBlock("filename", 1)
	block2 := types.NewBlock("filename", 2)
	block3 := types.NewBlock("filename", 1)
	block4 := types.NewBlock("filename2", 1)

	ts.True(block1.Equals(block1))
	ts.False(block1.Equals(block2))
	ts.True(block1.Equals(block3))
	ts.False(block1.Equals(block4))
}
