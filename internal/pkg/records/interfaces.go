package records

import "github.com/unhandled-exception/sophiadb/internal/pkg/types"

type trxInt interface {
	Pin(block types.Block) error
	BlockSize() uint32
	GetString(block types.Block, offset uint32) (string, error)
	GetInt64(block types.Block, offset uint32) (int64, error)
	GetInt8(block types.Block, offset uint32) (int8, error)
	SetString(block types.Block, offset uint32, value string, okToLog bool) error
	SetInt64(block types.Block, offset uint32, value int64, okToLog bool) error
	SetInt8(block types.Block, offset uint32, value int8, okToLog bool) error
}
