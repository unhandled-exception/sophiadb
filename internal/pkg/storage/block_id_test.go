package storage_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/storage"
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
	blockID := storage.NewBlockID(filename, blkNum)
	ts.Equal(filename, blockID.Filename())
	ts.Equal(blkNum, blockID.Number())
	ts.Equal("[file block_filename, block 12345]", blockID.String())
	ts.Equal("[block_filename][12345]", blockID.HashKey())
}

func (ts BlockIDTestSuite) TestBlockEquals() {
	block1 := storage.NewBlockID("filename", 1)
	block2 := storage.NewBlockID("filename", 2)
	block3 := storage.NewBlockID("filename", 1)
	block4 := storage.NewBlockID("filename2", 1)

	ts.True(block1.Equals(block1))
	ts.False(block1.Equals(block2))
	ts.True(block1.Equals(block3))
	ts.False(block1.Equals(block4))
}
