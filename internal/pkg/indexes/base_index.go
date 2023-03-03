package indexes

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

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

func (i *BaseIndex) Close() {
}

func (i *BaseIndex) BeforeFirst(searchKey scan.Constant) error {
	panic("not implemented") // TODO: Implement
}

func (i *BaseIndex) SearchCost(blocks int64, recordsPerBlock int64) int64 {
	panic("not implemented") // TODO: Implement
}

func (i *BaseIndex) Next() (bool, error) {
	panic("not implemented") // TODO: Implement
}

func (i *BaseIndex) RID() types.RID {
	panic("not implemented") // TODO: Implement
}

func (i *BaseIndex) Insert(value scan.Constant, rid types.RID) error {
	panic("not implemented") // TODO: Implement
}

func (i *BaseIndex) Delete(value scan.Constant, rid types.RID) error {
	panic("not implemented") // TODO: Implement
}
