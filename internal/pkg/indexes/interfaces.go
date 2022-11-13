package indexes

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type IndexType int8

const (
	HashIndexType  IndexType = 1
	BTreeIndexType IndexType = 2
)

var IndexTypeNames = map[IndexType]string{
	HashIndexType:  "hash",
	BTreeIndexType: "btree",
}

type Index interface {
	Type() IndexType
	Name() string
	Layout() records.Layout

	Close()
	SearchCost(blocks int64, recordsPerBlock int64) int64

	BeforeFirst(searchKey scan.Constant) error
	Next() (bool, error)
	RID() types.RID
	Insert(value scan.Constant, rid types.RID) error
	Delete(value scan.Constant, rid types.RID) error
}

const (
	IdxSchemaBlockField = "block"
	IdxSchemaIDField    = "id"
	IdxSchemaValueField = "dataval"
)
