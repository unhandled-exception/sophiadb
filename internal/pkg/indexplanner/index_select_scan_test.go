package indexplanner_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexplanner"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/transaction"
)

var _ scan.Scan = &indexplanner.IndexSelectScan{}

type IndexSelectScanTestSuite struct {
	Suite
}

func TestIndexSelectScanTestSuite(t *testing.T) {
	suite.Run(t, new(IndexSelectScanTestSuite))
}

func (ts *IndexSelectScanTestSuite) TestIndexSelectScan_HashIndex() {
	t := ts.T()

	tm, sm := ts.newTRXManager(defaultLockTimeout, "")
	defer func() {
		require.NoError(t, sm.Close())
	}()

	tableName := "table1"
	indexName := "index1"
	records := 1000

	ts.makeTestTable(tm, tableName, indexName, indexes.HashIndexType, records)
	ts.assertIndexedRecords(tm, tableName, indexName, records)
}

func (ts *IndexSelectScanTestSuite) makeTestTable(tm *transaction.TRXManager, tableName string, indexName string, it indexes.IndexType, records int) {
}

func (ts *IndexSelectScanTestSuite) assertIndexedRecords(tm *transaction.TRXManager, tableName string, indexName string, records int) {
	ts.Fail("add test")
}
