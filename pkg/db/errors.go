package db

import "github.com/pkg/errors"

var (
	ErrTransactionAlreadyStarted = errors.New("transaction already started")
	ErrBadDSN                    = errors.New("bad DSN")
)
