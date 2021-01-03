package storage

import (
	"fmt"
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
	blkNum := uint64(12345)
	blockID := NewBlockID(filename, blkNum)
	ts.Assert().Equal(filename, blockID.Filename())
	ts.Assert().Equal(blkNum, blockID.BlkNum())
	ts.Assert().Equal("[file block_filename, block 12345]", fmt.Sprintf("%s", blockID))
}

func (ts BlockIDTestSuite) TestBlockEquals() {
	block1 := NewBlockID("filename", 1)
	block2 := NewBlockID("filename", 2)
	block3 := NewBlockID("filename", 1)
	block4 := NewBlockID("filename2", 1)

	ts.Assert().True(block1.Equals(block1))
	ts.Assert().False(block1.Equals(block2))
	ts.Assert().True(block1.Equals(block3))
	ts.Assert().False(block1.Equals(block4))
}
