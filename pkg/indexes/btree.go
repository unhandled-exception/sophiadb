package indexes

import (
	"math"

	"github.com/unhandled-exception/sophiadb/pkg/records"
	"github.com/unhandled-exception/sophiadb/pkg/scan"
)

type BTreeIndex struct {
	*BaseIndex
}

func NewBTreeIndex(tx scan.TRXInt, idxName string, idxLayout records.Layout) (Index, error) {
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
