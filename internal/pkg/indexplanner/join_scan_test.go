package indexplanner_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexplanner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

var _ scan.Scan = &indexplanner.JoinScan{}

type JoinScanTestSute struct {
	Suite
}

func TestJoinScanTestSute(t *testing.T) {
	suite.Run(t, new(JoinScanTestSute))
}
