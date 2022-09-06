package scan_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

var (
	_ scan.Expression = scan.FieldExpression{}
	_ scan.Expression = scan.ScalarExpression{}
)

type ExpressionsTestSuite struct {
	suite.Suite
}

func TestExpressionsTestSuite(t *testing.T) {
	suite.Run(t, new(ExpressionsTestSuite))
}
