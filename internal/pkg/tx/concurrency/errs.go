package concurrency

import "github.com/pkg/errors"

var ErrConcurrency = errors.New("concurrency error")

var ErrLockAbort = errors.Wrap(ErrConcurrency, "failed to lock block")
