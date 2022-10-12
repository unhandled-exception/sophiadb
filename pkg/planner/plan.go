package planner

import (
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type Plan interface {
	Open() (scan.Scan, error)
	Schema() records.Schema
	BlocksAccessed() int64
	Records() int64
	DistinctValues(string) (int64, bool)
	String() string
}
