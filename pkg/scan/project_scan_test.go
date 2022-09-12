package scan_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type ProjectScanTestsuite struct {
	Suite
}

var _ scan.Scan = &scan.ProjectScan{}

func TestProjectScanTestsuite(t *testing.T) {
	suite.Run(t, new(ProjectScanTestsuite))
}
