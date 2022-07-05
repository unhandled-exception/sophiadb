package transaction

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/concurrency"
	"github.com/unhandled-exception/sophiadb/internal/pkg/tx/recovery"
)

type walManager interface {
	recovery.WALManager
}

type storageManager interface{}

type buffersManager interface {
	recovery.BuffersManager
}

type concurrencyManager interface {
	concurrency.ConcurrencyManager
}

type recoveryManager interface {
	recovery.RecoveryManager
}

type buffersList interface{}
