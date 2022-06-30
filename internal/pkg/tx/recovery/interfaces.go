package recovery

import "github.com/unhandled-exception/sophiadb/internal/pkg/types"

type trxInt interface {
	Pin(block *types.BlockID) error
	Unpin(block *types.BlockID) error
	SetString(block *types.BlockID, offset uint32, value string, okToLog bool) error
	SetInt64(block *types.BlockID, offset uint32, value int64, okToLog bool) error
}

type bufferManager interface{}

type walManager interface{}
