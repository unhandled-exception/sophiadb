package scan_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type ProductScanTestsuite struct {
	Suite
}

var _ scan.Scan = &scan.ProductScan{}

func TestProductScanTestsuite(t *testing.T) {
	suite.Run(t, new(PredicatesTestsuite))
}
