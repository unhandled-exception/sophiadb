package scan

import "github.com/pkg/errors"

var (
	ErrUpdateScanNotImplemented = errors.New("UpdateScan interface not implemented")
	ErrScan                     = errors.New("scan failed")
	ErrUnknownFieldType         = errors.New("unknown field type")
	ErrFieldNotFound            = errors.New("field not found")
)
