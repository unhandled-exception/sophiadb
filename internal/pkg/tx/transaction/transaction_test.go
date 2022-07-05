package transaction_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type TransactionTestSuite struct {
	suite.Suite
}

func TestTransactionTestSuite(t *testing.T) {
	suite.Run(t, new(TransactionTestSuite))
}
