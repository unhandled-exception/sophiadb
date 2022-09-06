package scan_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

var _ scan.Term = scan.EqualTerm{}

type TermsTestSuite struct {
	suite.Suite
}

func TestTermsTestSuite(t *testing.T) {
	suite.Run(t, new(TermsTestSuite))
}
