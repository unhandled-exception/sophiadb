package records

import "github.com/pkg/errors"

var (
	ErrRecordPage    = errors.New("record page error")
	ErrSlotNotFound  = errors.Wrap(ErrRecordPage, "slot not found")
	ErrFieldNotFound = errors.Wrap(ErrRecordPage, "field not found")

	ErrTableScan = errors.New("table scan failed")
)
