package indexes_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
)

var _ indexes.Index = &indexes.BTreeIndex{}

type BTreeIndexTestSuite struct {
	suite.Suite
}

func TestBTreeIndextestSuite(t *testing.T) {
	suite.Run(t, new(BTreeIndexTestSuite))
}
