package scan

import (
	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/types"
)

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

type Scan interface {
	Schema() records.Schema

	Close()
	BeforeFirst() error
	Next() (bool, error)

	HasField(fieldName string) bool
	GetInt64(fieldName string) (int64, error)
	GetInt8(fieldName string) (int8, error)
	GetString(fieldName string) (string, error)
	GetVal(fieldName string) (Constant, error)
}

type UpdateScan interface {
	Scan

	SetInt64(fieldName string, value int64) error
	SetInt8(fieldName string, value int8) error
	SetString(fieldName string, value string) error
	SetVal(fieldName string, value Constant) error

	Insert() error
	Delete() error
	RID() types.RID
	MoveToRID(rid types.RID) error
}

type Plan interface {
	DistinctValues(string) (int64, bool)
}
