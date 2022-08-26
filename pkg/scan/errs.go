package scan

import "github.com/pkg/errors"

var (
	ErrTableScan        = errors.New("table scan failed")
	ErrUnknownFieldType = errors.Wrap(ErrTableScan, "field not found")
)
