package indexes

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

const hashBuckets = 100

type HashIndex struct {
	*BaseIndex
}

func NewHashIndex(tx scan.TRXInt, idxName string, idxLayout records.Layout) (*HashIndex, error) {
	return &HashIndex{
		BaseIndex: &BaseIndex{
			idxType:   HashIndexType,
			idxName:   idxName,
			idxLayout: idxLayout,
		},
	}, nil
}

func HashIndexSearchCost(blocks int64, recordsPerBlock int64) int64 {
	return blocks / hashBuckets
}
