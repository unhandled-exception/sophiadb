package scan

import "github.com/unhandled-exception/sophiadb/pkg/types"

type TRXInt interface {
	Append(filename string) (types.Block, error)
	Pin(block types.Block) error
	Unpin(block types.Block)
	BlockSize() uint32
	GetString(block types.Block, offset uint32) (string, error)
	GetInt64(block types.Block, offset uint32) (int64, error)
	GetInt8(block types.Block, offset uint32) (int8, error)
	SetString(block types.Block, offset uint32, value string, okToLog bool) error
	SetInt64(block types.Block, offset uint32, value int64, okToLog bool) error
	SetInt8(block types.Block, offset uint32, value int8, okToLog bool) error
	Size(filename string) (types.BlockID, error)
}