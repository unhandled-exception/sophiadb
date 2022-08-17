package indexes

import (
	"math"

	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
)

type BTreeIndex struct {
	*BaseIndex
}

func NewBTreeIndex(tx records.TSTRXInt, idxName string, idxLayout records.Layout) (Index, error) {
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
