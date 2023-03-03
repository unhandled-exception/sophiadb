package indexes

import (
	"math"

	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type BTreeIndex struct {
	*BaseIndex
}

func NewBTreeIndex(tx scan.TRXInt, idxName string, idxLayout records.Layout) (*BTreeIndex, error) {
	return &BTreeIndex{
		BaseIndex: &BaseIndex{
			idxType:   BTreeIndexType,
			idxName:   idxName,
			idxLayout: idxLayout,
		},
	}, nil
}

func BTreeIndexSearchCost(blocks int64, recordsPerBlock int64) int64 {
	if blocks <= 0 {
		return 1
	}

	return 1 + int64(math.Round(math.Log(float64(blocks))/math.Log(float64(recordsPerBlock))))
}

func (i *BTreeIndex) Close() {
	panic("not implemented") // TODO: Implement
}

func (i *BTreeIndex) SearchCost(blocks int64, recordsPerBlock int64) int64 {
	panic("not implemented") // TODO: Implement
}

func (i *BTreeIndex) BeforeFirst(searchKey scan.Constant) error {
	panic("not implemented") // TODO: Implement
}

func (i *BTreeIndex) Next() (bool, error) {
	panic("not implemented") // TODO: Implement
}

func (i *BTreeIndex) RID() types.RID {
	panic("not implemented") // TODO: Implement
}

func (i *BTreeIndex) Insert(value scan.Constant, rid types.RID) error {
	panic("not implemented") // TODO: Implement
}

func (i *BTreeIndex) Delete(value scan.Constant, rid types.RID) error {
	panic("not implemented") // TODO: Implement
}
