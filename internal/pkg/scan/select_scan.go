package scan

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/types"
)

type SelectScan struct {
	s    Scan
	pred Predicate
}

func NewSelectScan(s Scan, pred Predicate) *SelectScan {
	return &SelectScan{
		s:    s,
		pred: pred,
	}
}

func (ss *SelectScan) Schema() records.Schema {
	return ss.s.Schema()
}

func (ss *SelectScan) Close() {
	ss.s.Close()
}

func (ss *SelectScan) BeforeFirst() error {
	return ss.s.BeforeFirst()
}

func (ss *SelectScan) Next() (bool, error) {
	for {
		ok, err := ss.s.Next()
		if !ok {
			if err != nil {
				return false, err
			}

			break
		}

		ok, err = ss.pred.IsSatisfied(ss.s)
		if err != nil {
			return false, err
		}

		if ok {
			return true, nil
		}
	}

	return false, nil
}

func (ss *SelectScan) HasField(fieldName string) bool {
	return ss.s.HasField(fieldName)
}

func (ss *SelectScan) GetInt64(fieldName string) (int64, error) {
	return ss.s.GetInt64(fieldName)
}

func (ss *SelectScan) GetInt8(fieldName string) (int8, error) {
	return ss.s.GetInt8(fieldName)
}

func (ss *SelectScan) GetString(fieldName string) (string, error) {
	return ss.s.GetString(fieldName)
}

func (ss *SelectScan) GetVal(fieldName string) (Constant, error) {
	return ss.s.GetVal(fieldName)
}

func (ss *SelectScan) SetInt64(fieldName string, value int64) error {
	us, ok := ss.s.(UpdateScan)
	if !ok {
		return ErrUpdateScanNotImplemented
	}

	return us.SetInt64(fieldName, value)
}

func (ss *SelectScan) SetInt8(fieldName string, value int8) error {
	us, ok := ss.s.(UpdateScan)
	if !ok {
		return ErrUpdateScanNotImplemented
	}

	return us.SetInt8(fieldName, value)
}

func (ss *SelectScan) SetString(fieldName string, value string) error {
	us, ok := ss.s.(UpdateScan)
	if !ok {
		return ErrUpdateScanNotImplemented
	}

	return us.SetString(fieldName, value)
}

func (ss *SelectScan) SetVal(fieldName string, value Constant) error {
	us, ok := ss.s.(UpdateScan)
	if !ok {
		return ErrUpdateScanNotImplemented
	}

	return us.SetVal(fieldName, value)
}

func (ss *SelectScan) Insert() error {
	us, ok := ss.s.(UpdateScan)
	if !ok {
		return ErrUpdateScanNotImplemented
	}

	return us.Insert()
}

func (ss *SelectScan) Delete() error {
	us, ok := ss.s.(UpdateScan)
	if !ok {
		return ErrUpdateScanNotImplemented
	}

	return us.Delete()
}

func (ss *SelectScan) RID() types.RID {
	us, ok := ss.s.(UpdateScan)
	if !ok {
		return types.RID{}
	}

	return us.RID()
}

func (ss *SelectScan) MoveToRID(rid types.RID) error {
	us, ok := ss.s.(UpdateScan)
	if !ok {
		return ErrUpdateScanNotImplemented
	}

	return us.MoveToRID(rid)
}
