package indexes

import "github.com/unhandled-exception/sophiadb/internal/pkg/records"

type BaseIndex struct {
	idxType   IndexType
	idxName   string
	idxLayout records.Layout
}

func (i *BaseIndex) Type() IndexType {
	return i.idxType
}

func (i *BaseIndex) Name() string {
	return i.idxName
}

func (i *BaseIndex) Layout() records.Layout {
	return i.idxLayout
}
