package indexes_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type BTreeDirTestSuite struct {
	Suite
}

func TestBTreeDirtestSuite(t *testing.T) {
	suite.Run(t, new(BTreeDirTestSuite))
}
