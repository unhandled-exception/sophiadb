package indexes

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

const defaultBucketsCount = 100

type StaticHashIndex struct {
	*BaseIndex

	bucketsCount int64
	searchKey    scan.Constant
	trx          scan.TRXInt
	ts           *scan.TableScan
}

func NewStaticHashIndex(trx scan.TRXInt, idxName string, idxLayout records.Layout) (*StaticHashIndex, error) {
	h := &StaticHashIndex{
		BaseIndex: &BaseIndex{
			idxType:   HashIndexType,
			idxName:   idxName,
			idxLayout: idxLayout,
		},
		bucketsCount: defaultBucketsCount,
		trx:          trx,
	}

	return h, nil
}

func HashIndexSearchCost(blocks int64, recordsPerBlock int64) int64 {
	return blocks / defaultBucketsCount
}

func (i *StaticHashIndex) BeforeFirst(searchKey scan.Constant) error {
	i.Close()

	bucket := int(searchKey.Hash() % uint64(i.bucketsCount))
	tableName := i.Name() + "_b_" + strconv.Itoa(bucket)

	ts, err := scan.NewTableScan(i.trx, tableName, i.Layout())
	if err != nil {
		return errors.WithMessage(ErrFailedToScanIndex, err.Error())
	}

	i.searchKey = searchKey
	i.ts = ts

	return nil
}

func (i *StaticHashIndex) Next() (bool, error) {
	for {
		ok, err := i.ts.Next()
		if err != nil {
			return false, errors.WithMessage(ErrFailedToScanIndex, err.Error())
		}

		if !ok {
			break
		}

		val, err := i.ts.GetVal(IdxSchemaValueField)
		if err != nil {
			return false, errors.WithMessage(ErrFailedToScanIndex, err.Error())
		}

		if i.searchKey.CompareTo(val) == scan.CompEqual {
			return true, nil
		}
	}

	return false, nil
}

func (i *StaticHashIndex) RID() types.RID {
	var rid types.RID

	if i.ts != nil {
		blockNumber, _ := i.ts.GetInt64(IdxSchemaBlockField)
		rid.BlockNumber = types.BlockID(blockNumber)

		slot, _ := i.ts.GetInt64(IdxSchemaIDField)
		rid.Slot = types.SlotID(slot)
	}

	return rid
}

func (i *StaticHashIndex) Insert(value scan.Constant, rid types.RID) error {
	if err := i.BeforeFirst(value); err != nil {
		return errors.WithMessage(ErrFailedToScanIndex, err.Error())
	}

	if err := i.ts.Insert(); err != nil {
		return errors.WithMessage(ErrFailedToScanIndex, err.Error())
	}

	if err := i.ts.SetInt64(IdxSchemaBlockField, int64(rid.BlockNumber)); err != nil {
		return errors.WithMessage(ErrFailedToScanIndex, err.Error())
	}

	if err := i.ts.SetInt64(IdxSchemaIDField, int64(rid.Slot)); err != nil {
		return errors.WithMessage(ErrFailedToScanIndex, err.Error())
	}

	if err := i.ts.SetVal(IdxSchemaValueField, value); err != nil {
		return errors.WithMessage(ErrFailedToScanIndex, err.Error())
	}

	return nil
}

func (i *StaticHashIndex) Delete(value scan.Constant, rid types.RID) error {
	if err := i.BeforeFirst(value); err != nil {
		return errors.WithMessage(ErrFailedToScanIndex, err.Error())
	}

	for {
		ok, err := i.Next()
		if err != nil {
			return errors.WithMessage(ErrFailedToScanIndex, err.Error())
		}

		if !ok {
			break
		}

		if i.RID() == rid {
			if err1 := i.ts.Delete(); err1 != nil {
				return errors.WithMessage(ErrFailedToScanIndex, err1.Error())
			}

			return nil
		}
	}

	return nil
}

func (i *StaticHashIndex) Close() {
	if i.ts != nil {
		i.ts.Close()
	}
}

func (i *StaticHashIndex) SearchCost(blocks int64, recordsPerBlock int64) int64 {
	return blocks / i.bucketsCount
}
