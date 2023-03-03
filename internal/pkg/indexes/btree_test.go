package indexes_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type BTreeIndexTestSuite struct {
	suite.Suite
}

func TestBTreeIndextestSuite(t *testing.T) {
	suite.Run(t, new(BTreeIndexTestSuite))
}
