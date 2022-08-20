package concurrency

import "github.com/unhandled-exception/sophiadb/pkg/types"

type Lockers interface {
	SLock(block types.Block) error
	XLock(block types.Block) error
	Unlock(block types.Block)
}

type ConcurrencyManager interface {
	SLock(block types.Block) error
	XLock(block types.Block) error
	Release()
}
