package indexes

import "github.com/unhandled-exception/sophiadb/pkg/records"

type IndexType int8

const (
	HashIndexType  IndexType = 1
	BTreeIndexType IndexType = 2
)

type Index interface {
	Type() IndexType
	Name() string
	Layout() records.Layout
}
