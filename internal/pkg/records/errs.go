package records

import "github.com/pkg/errors"

var (
	ErrRecordPage   = errors.New("record page error")
	ErrSlotNotFound = errors.New("slot not found")
)
