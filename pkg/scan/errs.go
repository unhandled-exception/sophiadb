package scan

import "github.com/pkg/errors"

var (
	ErrUpdateScanNotImplemented = errors.New("UpdateScan interface not implemented")
	ErrTableScan                = errors.New("table scan failed")
	ErrUnknownFieldType         = errors.New("unknown field type")
	ErrFieldNotFound            = errors.New("field not found")
)
