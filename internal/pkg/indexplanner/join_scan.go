package indexplanner

import (
	"github.com/unhandled-exception/sophiadb/internal/pkg/indexes"
	"github.com/unhandled-exception/sophiadb/internal/pkg/records"
	"github.com/unhandled-exception/sophiadb/internal/pkg/scan"
)

type JoinScan struct {
	lhs       scan.Scan
	rIdx      indexes.Index
	fieldName string
	rhs       *scan.TableScan
	schema    records.Schema
}

func NewJoinScan(lhs scan.Scan, rIdx indexes.Index, fieldName string, rhs *scan.TableScan) (*JoinScan, error) {
	s := &JoinScan{
		lhs:       lhs,
		rIdx:      rIdx,
		fieldName: fieldName,
		rhs:       rhs,
		schema:    records.NewSchema(),
	}

	s.schema.AddAll(lhs.Schema())
	s.schema.AddAll(rhs.Schema())

	return s, nil
}

func (s *JoinScan) Schema() records.Schema {
	return s.schema
}

func (s *JoinScan) Close() {
	s.lhs.Close()
	s.rIdx.Close()
	s.rhs.Close()
}

func (s *JoinScan) BeforeFirst() error {
	if err := s.lhs.BeforeFirst(); err != nil {
		return err
	}

	if _, err := s.lhs.Next(); err != nil {
		return err
	}

	if err := s.resetIndex(); err != nil {
		return err
	}

	return nil
}

func (s *JoinScan) Next() (bool, error) {
	for {
		ok, err := s.rIdx.Next()
		if err != nil {
			return false, err
		}

		if ok {
			if err1 := s.rhs.MoveToRID(s.rIdx.RID()); err1 != nil {
				return false, err1
			}

			break
		}

		ok, err = s.lhs.Next()
		if err != nil || !ok {
			return false, err
		}

		if err = s.resetIndex(); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (s *JoinScan) HasField(fieldName string) bool {
	return s.lhs.HasField(fieldName) || s.rhs.HasField(fieldName)
}

func (s *JoinScan) GetInt64(fieldName string) (int64, error) {
	if s.rhs.HasField(fieldName) {
		return s.rhs.GetInt64(fieldName)
	}

	return s.lhs.GetInt64(fieldName)
}

func (s *JoinScan) GetInt8(fieldName string) (int8, error) {
	if s.rhs.HasField(fieldName) {
		return s.rhs.GetInt8(fieldName)
	}

	return s.lhs.GetInt8(fieldName)
}

func (s *JoinScan) GetString(fieldName string) (string, error) {
	if s.rhs.HasField(fieldName) {
		return s.rhs.GetString(fieldName)
	}

	return s.lhs.GetString(fieldName)
}

func (s *JoinScan) GetVal(fieldName string) (scan.Constant, error) {
	if s.rhs.HasField(fieldName) {
		return s.rhs.GetVal(fieldName)
	}

	return s.lhs.GetVal(fieldName)
}

func (s *JoinScan) resetIndex() error {
	searchKey, err := s.lhs.GetVal(s.fieldName)
	if err != nil {
		return err
	}

	if err = s.rIdx.BeforeFirst(searchKey); err != nil {
		return err
	}

	return nil
}
