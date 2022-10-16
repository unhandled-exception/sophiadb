package recovery

import "github.com/pkg/errors"

var (
	ErrLogRecord        = errors.New("log record error")
	ErrEmptyLogRecord   = errors.Wrap(ErrLogRecord, "empty log record")
	ErrUnknownLogRecord = errors.Wrap(ErrLogRecord, "unknown log record")
	ErrBadLogRecord     = errors.Wrap(ErrLogRecord, "bad log record")

	ErrOpError = errors.New("recovery operation failed")
)
