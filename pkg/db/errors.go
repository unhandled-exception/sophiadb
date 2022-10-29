package db

import "github.com/pkg/errors"

var (
	ErrFailedProcessPlaceholders = errors.New("failed to process placeholders")
	ErrUnserializableValue       = errors.New("unserializable value")
	ErrTransactionAlreadyStarted = errors.New("transaction already started")
	ErrBadDSN                    = errors.New("bad DSN")
)
