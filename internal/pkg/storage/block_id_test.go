package storage

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type BlockIDTestSuite struct {
	suite.Suite
}

func TestBlockIDTestSuite(t *testing.T) {
	suite.Run(t, new(BlockIDTestSuite))
}

func (ts BlockIDTestSuite) TestCreateBlockID() {
	filename := "block_filename"
	blkNum := uint32(12345)
	blockID := NewBlockID(filename, blkNum)
	ts.Equal(filename, blockID.Filename())
	ts.Equal(blkNum, blockID.Number())
	ts.Equal("[file block_filename, block 12345]", blockID.String())
	ts.Equal("[block_filename][12345]", blockID.HashKey())
}

func (ts BlockIDTestSuite) TestBlockEquals() {
	block1 := NewBlockID("filename", 1)
	block2 := NewBlockID("filename", 2)
	block3 := NewBlockID("filename", 1)
	block4 := NewBlockID("filename2", 1)

	ts.True(block1.Equals(block1))
	ts.False(block1.Equals(block2))
	ts.True(block1.Equals(block3))
	ts.False(block1.Equals(block4))
}
