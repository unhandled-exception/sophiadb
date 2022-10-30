package planner

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type Plan interface {
	Open() (scan.Scan, error)
	Schema() records.Schema
	BlocksAccessed() int64
	Records() int64
	DistinctValues(string) (int64, bool)
	String() string
}
