package scan_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

var (
	_ scan.Scan       = &scan.SelectScan{}
	_ scan.UpdateScan = &scan.SelectScan{}
)

type SelectScanTestSuite struct {
	Suite
}

func TestSelectScanTestsuite(t *testing.T) {
	suite.Run(t, new(SelectScanTestSuite))
}
