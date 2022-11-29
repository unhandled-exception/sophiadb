package indexplanner

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type IndexSelectScan struct {
	ts    *scan.TableScan
	idx   indexes.Index
	value scan.Constant
}

func NewIndexSelectScan(ts *scan.TableScan, idx indexes.Index, value scan.Constant) (*IndexSelectScan, error) {
	s := &IndexSelectScan{
		ts:    ts,
		idx:   idx,
		value: value,
	}

	return s, nil
}

func (ss *IndexSelectScan) Schema() records.Schema {
	return ss.ts.Schema()
}

func (ss *IndexSelectScan) Close() {
	ss.idx.Close()
	ss.ts.Close()
}

func (ss *IndexSelectScan) BeforeFirst() error {
	return ss.idx.BeforeFirst(ss.value)
}

func (ss *IndexSelectScan) Next() (bool, error) {
	ok, err := ss.idx.Next()
	if err != nil {
		return false, err
	}

	if ok {
		if err = ss.ts.MoveToRID(ss.idx.RID()); err != nil {
			return false, err
		}
	}

	return ok, nil
}

func (ss *IndexSelectScan) HasField(fieldName string) bool {
	return ss.ts.HasField(fieldName)
}

func (ss *IndexSelectScan) GetInt64(fieldName string) (int64, error) {
	return ss.ts.GetInt64(fieldName)
}

func (ss *IndexSelectScan) GetInt8(fieldName string) (int8, error) {
	return ss.ts.GetInt8(fieldName)
}

func (ss *IndexSelectScan) GetString(fieldName string) (string, error) {
	return ss.ts.GetString(fieldName)
}

func (ss *IndexSelectScan) GetVal(fieldName string) (scan.Constant, error) {
	return ss.ts.GetVal(fieldName)
}
